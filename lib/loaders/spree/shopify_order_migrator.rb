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

    class ShopifyOrderLoader < CsvLoader
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

          CREATED_RECORDS = [
            :order, :ship_address, :bill_address, :user,
            :payment, :payment_method, :zone, :zone_members,
            :tax_related_records, :line_items, :shipping_category,
            :shipping_method, :shipment_related_records
          ]

          class TaxRelated
            ATTRS = [:tax_rate, :zone, :zone_members, :adjustment, :tax_category]

            attr_accessor *ATTRS
          end

          class ShipmentRelated
            ATTRS = [:shipment, :shipment_rate]

            attr_accessor *ATTRS
          end

          NUM_TAXES = 5

          attr_accessor *HEADERS
          attr_accessor *CREATED_RECORDS
          attr_accessor :raw_line_items

          def initialize
            @tax_related_records = []
            @shipment_related_records = []
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
            }
            ).create!
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
            }
            ).create!
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
                tax_category = Spree::TaxCategory.where(name: tax_category_name).first_or_create!
                zone = Spree::Zone.where(name: shipping_country).first_or_create!
                zone_members = zone.zone_members.create!(zoneable: Spree::Country.find_by!(iso: shipping_country))
                tax_rate_attributes = {
                  name: "Shopify-#{shipping_country}-#{tax_category_name}",
                  amount: tax_category_value/amount,
                  zone: zone,
                  tax_category: tax_category
                }
                tax_rate = Spree::TaxRate.where(tax_rate_attributes).first
                unless tax_rate
                  tax_rate = Spree::TaxRate.create(tax_rate_attributes)
                  tax_rate.calculator = Spree::Calculator::DefaultTax.create!
                  tax_rate.save!
                end
                t = TaxRelated.new
                t.tax_category = tax_category
                t.zone = zone
                t.zone_members = zone_members
                t.tax_rate = tax_rate
                t.adjustment = make_tax_adjustments(t.tax_rate, order, tax_category_value)
                self.tax_related_records << t
              end
            end
          end

          def create_shipping_method
            zone = Spree::Zone.where(name: shipping_country).first_or_create!
            zone_members = zone.zone_members.create!(zoneable: Spree::Country.find_by!(iso: shipping_country))
            shipping_category = Spree::ShippingCategory.where(name: get_shipping_method).first_or_create!
            shipping_method = Spree::ShippingMethod.where(name: get_shipping_method).first
            # Does tax_category makes sense here ??
            unless shipping_method
              shipping_method = Spree::ShippingMethod.create!({
                name: get_shipping_method,
                zones: [zone],
                calculator: Spree::Calculator::Shipping::FlatRate.create!, # Check the values needed
                shipping_categories: [shipping_category]
              })
            end
            self.zone = zone
            self.zone_members = zone_members
            self.shipping_category = shipping_category
            self.shipping_method = shipping_method
            shipping_method
          end

          def get_shipping_method
            ((shipping_method == "") || (shipping_method.nil?)) ? "Manual" : shipping_method
          end

          def make_payment
            attributes = {name: "Shopify Import", active: true, type: "Spree::PaymentMethod::Check"} # Add New Type to signify Shopify  # Rails.application.config.spree.payment_methods?
            method = Spree::PaymentMethod.where(attributes).first_or_create!
            payment = order.payments.create!(:amount => total, :payment_method => method)
            payment.update_columns(:state => 'completed')
            self.payment = payment
            self.payment_method = method
          end

          def make_shipments_and_ship
            shipping_method = create_shipping_method
            order.create_proposed_shipments
            # Shipping Methods are picked from Products and not here :(
            order.shipments.each do |shipment|
              # It resulted in creation of shipping rates again and failed on unique indexes :(
              #rate = shipment.shipping_rates.create!(shipping_method: shipping_method, cost: 0)
              #shipment.selected_shipping_rate_id = rate.id
              shipment.update_columns(state: "shipped", shipped_at: fulfilled_at)
              t = ShipmentRelated.new
              t.shipment_rate = shipment.selected_shipping_rate
              t.shipment = shipment
              self.shipment_related_records << t
            end
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
              :number => "Shopify-#{name.gsub('#', '')}",
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

      def perform_load(options = {})

        order_patch_for_no_mails

        Spree::Config[:track_inventory_levels] = false
        # headers = "Name","Email","Financial Status","Paid at","Fulfillment Status","Fulfilled at",
        # "Accepts Marketing","Currency","Subtotal","Shipping","Taxes","Total","Discount Code",
        # "Discount Amount","Shipping Method","Created at","Lineitem quantity","Lineitem name",
        # "Lineitem price","Lineitem compare at price","Lineitem sku","Lineitem requires shipping",
        # "Lineitem taxable","Lineitem fulfillment status","Billing Name","Billing Street",
        # "Billing Address1","Billing Address2","Billing Company","Billing City","Billing Zip",
        # "Billing Province","Billing Country","Billing Phone","Shipping Name","Shipping Street",
        # "Shipping Address1","Shipping Address2","Shipping Company","Shipping City","Shipping Zip",
        # "Shipping Province","Shipping Country","Shipping Phone","Notes","Note Attributes",
        # "Cancelled at","Payment Method","Payment Reference","Refunded Amount","Vendor","Id",
        # "Tags","Risk Level","Source","Lineitem discount","Tax 1 Name","Tax 1 Value",
        # "Tax 2 Name","Tax 2 Value","Tax 3 Name","Tax 3 Value","Tax 4 Name","Tax 4 Value",
        # "Tax 5 Name","Tax 5 Value"
        order_count = 0
        last_order = nil
        CSV.foreach(file_name, headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
          if(row.empty?)
            logger.info "Finished - Last Row #{row}"
            break
          end

          name = row.fetch(:name)
          financial_status = row.fetch(:financial_status)
          if(!name.nil? && !name.empty?) && (financial_status.nil? || financial_status.empty?)   # Financial Status empty on LI rows
            logger.info "Process Line Item"
            last_order.raw_line_items << Shopify::RawOrder.line_item_init(row)
          else
            if last_order
              logger.info %Q{ process last order - #{last_order.name} }
              finish_order(last_order)
            end
            order_count += 1
            logger.info %Q{ Create Next Order - #{row["Name"]} }
            o = Shopify::RawOrder.init(row)
            last_order = o
            last_order.raw_line_items << Shopify::RawOrder.line_item_init(row)
          end
        end

        logger.info "Process Last Order - #{last_order.name}"
        finish_order(last_order)
        logger.info "Order Count #{order_count}"
      ensure
        Spree::Config[:track_inventory_levels] = true
      end

      private

        def order_patch_for_no_mails
          Spree::Order.class_eval do
            def confirmation_required?
              false
            end

            def payment_required?
              false
            end
          end
        end

        def finish_order(last_order)
          if((last_order.fulfillment_status == "fulfilled") && (last_order.financial_status == "paid"))
            begin
              logger.info "ShopifyOrderImport :: #{last_order.name} Started Import for Order"
              Spree::Order.transaction do
                logger.info "ShopifyOrderImport ::  #{last_order.name} :: Create Order"
                last_order.make_order
                logger.info "ShopifyOrderImport ::  #{last_order.name} :: Create Line Items"
                last_order.add_line_items
                logger.info "ShopifyOrderImport ::  #{last_order.name} :: Create Tax Adjustments"
                last_order.make_tax_category_and_rate
                last_order.order = last_order.order.reload
                logger.info "ShopifyOrderImport ::  #{last_order.name} :: Create Shipments"
                last_order.make_shipments_and_ship

                logger.info "ShopifyOrderImport ::  #{last_order.name} :: Create Payment"
                last_order.make_payment

                logger.info "ShopifyOrderImport ::  #{last_order.name} :: Finalise Order"
                last_order.order.state = "complete"
                last_order.order.payment_state = "paid"
                last_order.order.shipment_state = "shipped"
                last_order.order.completed_at = Time.now - 1.day
                last_order.order.save!
              end
            rescue => e
              logger.error "ShopifyOrderImport :: #{last_order.name} :: Order Processing failed for #{last_order.name} with reason #{e}"
              raise e if true ## Add a halt/no-halt variable to stop execution at failure
            ensure
              logger.info "ShopifyOrderImport :: #{last_order.name} :: Finishing Log Import for Order #{last_order.name}"
            end
          else
            logger.info "ShopifyOrderImport :: #{last_order.name} :: Order Skipped as either not paid or not fullfilled #{last_order.name} :: #{last_order.financial_status} => #{last_order.fulfillment_status}"
          end
        end
    end
  end
end
