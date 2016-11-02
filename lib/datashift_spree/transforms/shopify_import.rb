require 'csv'

String.class_eval do
  def present?
    !(self == "")
  end
end
module DataShift
  module SpreeEcom
    class ShopifyProductTransform

      HEADERS = [:slug, :price, :name, :description, :meta_title, :meta_description, :meta_keywords, :available_on,
                 :images, :shipping_category, :tax_category, :weight, :sku, :variants, :variant_sku,
                 :variant_weight, :variant_price, :variant_cost_price, :variant_images, :stock_items, :gift_card]

      attr_reader :filename

      def initialize(filename)
        @filename = filename
      end

      def option_names
        {
          :option1_name => :option1_value,
          :option2_name => :option2_value,
          :option3_name => :option3_value
        }
      end

      def get_variants_option_values(row, res)
        t = []
        if res.nil?
          res = {options: []}
        end
        option_names.each do |k, v|
          if(row[k] && row[k].present?)
            res[k] = row[k]
          end
          if(row[v] && row[v].present?)
            t.push("#{res[k]}:#{row[v]}")
          end
        end
        res[:options] << t.join(";")
        res
      end

      def read_and_interpret_csv
        start_handle = "ZIBBERISh_LOREM_IPSUM"
        a = []
        CSV.foreach(filename, headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
          if(start_handle == row[:handle])
            p "Its Variant Data"
            if(row[:variant_sku] && row[:variant_sku].present?)
              p "Process variant Data"
              last_product = a.last
              last_product[:variants] = get_variants_option_values(row, last_product[:variants])
              last_product[:variant_sku] = last_product[:variant_sku].push(row[:variant_sku])
              last_product[:variant_weight] = last_product[:variant_weight].push(row[:variant_grams])
              last_product[:variant_price] = last_product[:variant_price].push(row[:variant_price])
              last_product[:variant_cost_price] = last_product[:variant_cost_price].push(row[:variant_compare_at_price])
              last_product[:variant_images] = last_product[:variant_images].push(row[:variant_image])
              last_product[:stock_items] = last_product[:stock_items].push("#{row[:variant_inventory_tracker]}:#{row[:variant_inventory_qty]}")
            else
              p "Skip variant Data"
            end
          else
            unless (row[:title] && row[:title].present?)
              p "Invalid Product Data"
              next
            end
            p "Stop and Process"
            start_handle = row[:handle]
            p "Its New Product Data"
            p row.to_hash

            a.push({
              slug: row[:handle], name: row[:title], description: row[:body_html],
              meta_title: row[:seo_title], meta_description: row[:seo_description],
              meta_keywords: "#{row[:handle]}, #{row[:title]}, the Vinsol",
              available_on: Time.now,
              images: row[:image_src],
              price: row[:variant_price], cost_price: row[:variant_compare_at_price],
              shipping_category: ((row[:variant_requires_shipping] == "TRUE") ? "Shopify-#{row[:variant_fulfillment_service]}-Shipping Category" : "Shopify-OTHER-Shipping Category"),
              tax_category: ((row[:variant_taxable] == "TRUE") ? "Shopify-TRUE-#{row[:variant_tax_code]}-Tax Category" : "Shopify-OTHER-Tax Category"),
              weight: row[:variant_grams],
              sku: row[:variant_sku],
              variants: ((row[:option1_name] == "Title") ? {options: []} : get_variants_option_values(row, nil)),
              variant_sku: [row[:variant_sku]],
              variant_weight: [row[:variant_grams]],
              variant_price: [row[:variant_price]],
              variant_cost_price: [row[:variant_compare_at_price]],
              variant_images: [row[:variant_image]],
              stock_items: ["#{row[:variant_inventory_tracker]}:#{row[:variant_inventory_qty]}"],
              gift_card: row[:gift_card]
            })
          end
        end

        a.each do |r|
          if(r[:variants][:options].size > 0)
            r[:variants] = r[:variants][:options].join("|")
            r[:variant_sku] = r[:variant_sku].join("|")
            r[:variant_weight] = r[:variant_weight].join("|")
            r[:variant_price] = r[:variant_price].join("|")
            r[:variant_cost_price] = r[:variant_cost_price].join("|")
            r[:variant_images] = r[:variant_images].join("|")
          else
            r[:variants] = ""
            r[:variant_sku] = ""
            r[:variant_weight] = ""
            r[:variant_price] = ""
            r[:variant_cost_price] = ""
            r[:variant_images] = ""
          end
          r[:stock_items] = r[:stock_items].join("|")
        end
        a
      end

      def to_csv
        a = read_and_interpret_csv
        csv_header_data = CSV.generate do |csv|
          b = []
          HEADERS.each do |h|
            b << h.to_s
          end
          csv << b
        end
        csv_row_data = CSV.generate do |csv|
          a.each do |r|
            b = []
            HEADERS.each do |h|
              b << r.fetch(h)
            end
            csv << b
          end
        end
        csv_header_data + csv_row_data
      end
    end
  end
end
