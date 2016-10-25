# Copyright:: (c) Autotelik Media Ltd 2015
# Author ::   Tom Statter
# Date ::     Aug 2015
# License::   MIT
#
# Details::   Supports migrating Shopify spreadsheets to Spree
#               Currently covers :
#                 Orders
#
require 'spree_base_loader'
require 'spree_ecom'

module DataShift

  module SpreeEcom

    class ShopifyOrderLoader < SpreeBaseLoader
      class CacheOrderData
        attr_accessor :shipping_method, :tax_category, :tax_rate, :row
      end

      module Shopify
        class RawOrder
          ORDER_HEADERS = [:name, :email, :financial_status, :paid_at, :fulfillment_status, :fulfilled_at, :accepts_marketing,
                     :currency, :subtotal, :shipping, :taxes, :total, :discount_code, :discount_amount, :shipping_method,
                     :created_at, :notes, :note_attributes, :cancelled_at, :payment_method, :payment_reference,
                     :refunded_amount, :vendor, :id, :tags, :risk_level, :source]

          LINE_ITEM_HEADERS = [:lineitem_quantity, :lineitem_name, :lineitem_price, :lineitem_compare_at_price,
                     :lineitem_sku, :lineitem_requires_shipping, :lineitem_taxable, :lineitem_fulfillment_status, :lineitem_discount]

          BILLING_HEADERS = [:billing_name, :billing_street, :billing_address1, :billing_address2, :billing_company, :billing_city,
                     :billing_zip, :billing_province, :billing_country, :billing_phone]

          SHIPPING_HEADERS = [:shipping_name, :shipping_street,
                     :shipping_address1, :shipping_address2, :shipping_company, :shipping_city, :shipping_zip, :shipping_province,
                     :shipping_country, :shipping_phone]

          TAX_HEADERS = [:tax_1_name, :tax_1_value, :tax_2_name, :tax_2_value, :tax_3_name, :tax_3_value,
                          :tax_4_name, :tax_4_value, :tax_5_name, :tax_5_value]

          HEADERS = ORDER_HEADERS + LINE_ITEM_HEADERS+ BILLING_HEADERS + SHIPPING_HEADERS + TAX_HEADERS

          CREATED_RECORDS = [:order, :ship_address, :bill_address, :user, :payment]

          class TaxRelated
            ATTRS = [:shipping_category, :shipping_method, :tax_category, :tax_rate, :zone, :adjustment]

            attr_accessor *ATTRS
          end

          NUM_TAXES = 5

          attr_accessor *HEADERS
          attr_accessor *CREATED_RECORDS
          attr_accessor :tax_related_records, :line_items, :raw_line_items

          def initialize
            @tax_related_records = []
            @line_items = []
            @raw_line_items = []
          end

          def self.line_item_init(h)
            obj = new
            LINE_ITEM_HEADERS.each do |k|
              obj.send("#{k}=", h.fetch(k))
            end
            obj
          end

          def self.init(h)
            obj = new
            HEADERS.each do |k|
              obj.send("#{k}=", h.fetch(k))
            end
            obj
          end

          def make_shipping_address
            self.ship_address = Spree::Address.where(
            {
              firstname: shipping_name,
              lastname: shipping_name,
              address1: shipping_address1,
              address2: shipping_address2,
              city: shipping_city,
              zipcode: shipping_zip,
              phone: shipping_phone,
              state_name: shipping_province,
              company: shipping_company,
              country: Spree::Country.where(iso: shipping_country).first,
              state: Spree::State.where(abbr: shipping_province).first
            }
            ).first_or_create!
          end

          def make_billing_address
            self.bill_address = Spree::Address.where(
            {
              firstname: billing_name,
              lastname: billing_name,
              address1: billing_address1,
              address2: billing_address2,
              city: billing_city,
              zipcode: billing_zip,
              phone: billing_phone,
              state_name: billing_province,
              company: billing_company,
              country: Spree::Country.where(iso: billing_country).first,
              state: Spree::State.where(abbr: billing_province).first
            }
            ).first_or_create!
          end

          def make_user
            self.user = Spree::User.where(email: email).first_or_create!
          end

          def make_tax_adjustments(tax_rate, order, amount)
            order.adjustments.create!(
              :amount => amount,
              :source => tax_rate,
              :order  => order,
              :label => "Tax",
              :state => "closed",
              :mandatory => true)
          end

          def make_tax_category_and_rate
            (1..NUM_TAXES).each do |i|
              if(self.send(:"tax_#{i}_name")).present?
                amount = subtotal.to_f
                tax_category_name = self.send(:"tax_#{i}_name")
                tax_category_value = self.send(:"tax_#{i}_value").to_f
                t = TaxRelated.new
                t.tax_category = Spree::TaxCategory.where(name: tax_category_name).first_or_create!
                t.zone = Spree::Zone.where(name: shipping_country).first_or_create!
                tax_rate_attributes = { name: "Shopify-#{shipping_country}-#{tax_category_name}", amount: tax_category_value/amount, zone: t.zone, tax_category: t.tax_category }
                tax_rate = Spree::TaxRate.where(tax_rate_attributes).first
                unless tax_rate
                  tax_rate = Spree::TaxRate.create(tax_rate_attributes)
                  tax_rate.calculator = Spree::Calculator::DefaultTax.create!
                  tax_rate.save!
                end
                t.tax_rate = tax_rate
                t.adjustment = make_tax_adjustments(t.tax_rate, order, tax_category_value)
                t.shipping_method, t.shipping_category = create_shipping_method(t.tax_category)
                self.tax_related_records << t
              end
            end
          end

          def create_shipping_method(tax_category)
            shipping_category = Spree::ShippingCategory.where(name: get_shipping_method).first_or_create!
            shipping_method = Spree::ShippingMethod.first ##shipping_category.shipping_methods.where(name: get_shipping_method, tax_category: tax_category).first_or_create!
            [shipping_method, shipping_category]
          end

          def get_shipping_method
            shipping_method || "Manual"
          end

          def make_payment
            attributes = {name: "Shopify Import", active: true}
            method = Spree::PaymentMethod.where(attributes).first_or_create!
            self.payment = order.payments.create!(:amount => total, :payment_method => method)
            payment.update_columns(:state => 'completed')
          end

          def add_line_items
            self.raw_line_items.each do |l|
              self.line_items << self.order.line_items.create!(
                :variant => Spree::Variant.find_by_sku!(l.lineitem_sku),
                :quantity => l.lineitem_quantity,
                :price => l.lineitem_price,
                :cost_price => l.lineitem_compare_at_price,
                :promo_total => l.lineitem_discount
              )
            end
          end

          def make_order
            self.order = Spree::Order.create!(
              :number => "Shopify#{name}",
              :email => email,
              :item_total => subtotal,
              # :adjustment_total => (taxes + discount_amount),
              :shipment_total => shipping,
              :additional_tax_total => taxes,
              :promo_total => discount_amount,
              :total => total,
              :shipping_address => make_shipping_address,
              :billing_address => make_billing_address,
              :user => make_user
            )
          end
        end
      end

      # Options
      #
      #  :reload           : Force load of the method dictionary for object_class even if already loaded
      #  :verbose          : Verbose logging and to STDOUT
      #
      def initialize(klass, options = {})
        # We want the delegated methods so always include instance methods
        opts = {:instance_methods => true }.merge( options )

        super( klass, nil, opts)

        raise "Failed to create a #{klass.name} for loading" unless load_object
      end

      # OVER RIDES

      # Options:
      #   [:dummy]           : Perform a dummy run - attempt to load everything but then roll back
      #
      def perform_load( file_name, opts = {} )
        logger.info "Shopify perform_load for Orders from File [#{file_name}]"
        super(file_name, opts)
      end

      def perform_csv_load(file_name, options = {})
        Spree::Config[:track_inventory_levels] = false
        # headers = "Name","Email","Financial Status","Paid at","Fulfillment Status","Fulfilled at","Accepts Marketing","Currency","Subtotal","Shipping","Taxes","Total","Discount Code","Discount Amount","Shipping Method","Created at","Lineitem quantity","Lineitem name","Lineitem price","Lineitem compare at price","Lineitem sku","Lineitem requires shipping","Lineitem taxable","Lineitem fulfillment status","Billing Name","Billing Street","Billing Address1","Billing Address2","Billing Company","Billing City","Billing Zip","Billing Province","Billing Country","Billing Phone","Shipping Name","Shipping Street","Shipping Address1","Shipping Address2","Shipping Company","Shipping City","Shipping Zip","Shipping Province","Shipping Country","Shipping Phone","Notes","Note Attributes","Cancelled at","Payment Method","Payment Reference","Refunded Amount","Vendor","Id","Tags","Risk Level","Source","Lineitem discount","Tax 1 Name","Tax 1 Value","Tax 2 Name","Tax 2 Value","Tax 3 Name","Tax 3 Value","Tax 4 Name","Tax 4 Value","Tax 5 Name","Tax 5 Value"
        order_count = 0
        last_order = nil
        CSV.foreach(file_name, headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
        #CSV.foreach(file_name, headers: true, encoding: 'ISO-8859-1') do |row|
          if(row.empty?)
            puts "Finished - Last Row #{row}"
            break
          end

          name = row.fetch(:name)
          financial_status = row.fetch(:financial_status)
          if(!name.nil? && !name.empty?) && (financial_status.nil? || financial_status.empty?)   # Financial Status empty on LI rows
            p "Process Line Item"
            last_order.raw_line_items << Shopify::RawOrder.line_item_init(row)
          else
            if last_order
              p %Q{ process last order - #{last_order.name} }
              begin
                Spree::Order.transaction do
                  last_order.make_order
                  last_order.add_line_items
                  last_order.make_tax_category_and_rate
                  last_order.order = last_order.order.reload
                  last_order.order.create_proposed_shipments
                  last_order.make_payment

                  last_order.order.state = "complete"
                  last_order.order.payment_state = "paid"
                  last_order.order.shipment_state = "shipped"
                  last_order.order.completed_at = Time.now - 1.day
                  last_order.order.save!
                end
              rescue => e
                p "Order Processing failed for #{last_order.name} with reason #{e}"
                raise e
              end
            end
            order_count += 1
            p %Q{ Create Next Order - #{row["Name"]} }
            o = Shopify::RawOrder.init(row)
            last_order = o
            last_order.raw_line_items << Shopify::RawOrder.line_item_init(row)
          end
        end

        p "Process Last Order - #{last_order}"
        p "Order Count #{order_count}"
      end

      private

        def cached_order_data
          @cached_order_data ||= CacheOrderData.new
        end

        def reset_cached_order_data
          @cached_order_data = nil
        end

        def create_order
          order = Spree::Order.create!(
            :number => "Shopify#{row["Name"]}",
            :email => row["Email"],
            :item_total => row["Subtotal"],
            :adjustment_total => 150.95,
            :shipment_total => row["Shipping"],
            :additional_tax_total => row["Taxes"],
            :promo_total => row["Discount Amount"],
            :total => row["Total"],
            :shipping_method => cached_order_data.shipping_method,
            :shipping_address => create_address("Shipping"),
            :billing_address => create_address("Billing"),
            :user => create_user
          )
        end

        def create_user
          Spree::User.where(email: row["Email"]).first_or_create!
        end

        def create_address(name)
          Spree::Address.where(address_attributes(name)).first_or_create!
        end

        def address_attributes(prefix)
          {
            firstname: row["#{prefix} Name"],
            lastname: row["#{prefix} Name"],
            address1: row["#{prefix} Address1"],
            address2: row["#{prefix} Address2"],
            city: row["#{prefix} City"],
            zipcode: row["#{prefix} Zip"],
            phone: row["#{prefix} Phone"],
            state_name: row["#{prefix} Province"],
            company: row["#{prefix} Company"],
            country: Spree::Country.where(iso: row["#{prefix} Country"]).first,
            state: Spree::State.where(abbr: row["#{prefix} Province"]).first
          }
        end

        def create_line_item(order)
          order.line_items.create!(
            :variant => Spree::Variant.find_by_sku!(row[get_line_item_variant_sku_col]),
            :quantity => row[get_line_item_qty_col],
            :price => row[get_line_item_price_col],
            :cost_price => row[get_line_item_cost_price_col])
        end

        def get_line_item_variant_sku_col
          "Lineitem sku"
        end

        def get_line_item_qty_col
          "Lineitem quantity"
        end

        def get_line_item_price_col
          "Lineitem price"
        end

        def get_line_item_cost_price_col
          "Lineitem compare at price"
        end

        def process_order
          order.create_proposed_shipments

          order.state = "complete"
          order.completed_at = Time.now - 1.day
          order.save!
        end

        def create_tax_adjustments(tax_rate, order, amount)
          order.adjustments.create!(
            :amount => amount,
            :source => tax_rate,
            :order  => order,
            :label => "Tax",
            :state => "closed",
            :mandatory => true)
        end

        def create_tax_category_and_rate(row, order)
          (1..5).each do |i|
            if(row[tax_category_col_name]).present?
              tax_category = Spree::TaxCategory.where(name: row[tax_category_col_name(i)]).first_or_create!
              zone = Spree::Zone.where(name: row[shipping_country_name]).first_or_create!
              amount = row[order_subtotal_col].to_f
              tax_rate = tax_category.tax_rates.where(amount: amount/row[tax_category_col_name(i)], zone: zone).first_or_create!
              create_tax_adjustments(tax_rate, order, amount)
              create_shipping_method
            end
          end
        end

        def create_shipping_method(tax_category)
          shipping_category = Spree::ShippingCategory.where(name: row[shipping_method_col_name]).first_or_create!
          shipping_method = shipping_category.shipping_methods.where(name: row[shipping_method_col_name], tax_category: tax_category).first_or_create!
        end

        def shipping_method_col_name
          "Shipping Method"
        end

        def tax_category_col_name(i)
          "Tax #{i} Name"
        end

        def tax_category_col_name(i)
          "Tax #{i} Value"
        end

        def order_subtotal_col
          "Subtotal"
        end

        def shipping_country_name
          "Shipping Country"
        end

        def create_payment
          method = create_payment_method
          payment = order.payments.create!(:amount => order.total, :payment_method => method)
          payment.update_columns(:state => 'completed')
        end

        def create_payment_method
          attributes = {name: "Shopify Import", active: true}
          method = Spree::PaymentMethod.where(attributes).first
          unless method
            method = Spree::PaymentMethod.create!(attributes)
          end
          method
        end
    end
  end
end
