require 'csv'

def option_names
  {
    :option1_name => :option1_value,
    :option2_name => :option2_value,
    :option3_name => :option3_value
  }
end

String.class_eval do
  def present?
    !(self == "")
  end
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

pt = "/Users/pikender/spree/marketing/products-import/CSV samples/Shopify/products/products_export_variants-copy.csv"
start_handle = "ZIBBERISh_LOREM_IPSUM"
a = []
CSV.foreach(pt, headers: true, header_converters: :symbol, encoding: 'ISO-8859-1') do |row|
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
  r[:variants] = r[:variants][:options].join("|")
  r[:variant_sku] = r[:variant_sku].join("|")
  r[:variant_weight] = r[:variant_weight].join("|")
  r[:variant_price] = r[:variant_price].join("|")
  r[:variant_cost_price] = r[:variant_cost_price].join("|")
  r[:variant_images] = r[:variant_images].join("|")
  r[:stock_items] = r[:stock_items].join("|")
end

headers = [:slug, :name, :description, :meta_title, :meta_description, :meta_keywords, :available_on,
           :images, :price, :shipping_category, :tax_category, :weight, :sku, :variants, :variant_sku,
           :variant_weight, :variant_price, :variant_cost_price, :variant_images, :stock_items, :gift_card]

CSV.open("/Users/pikender/Desktop/myfile.csv", "w") do |csv|
  b = []
  headers.each do |h|
    b << h.to_s
  end
  csv << b
end
CSV.open("/Users/pikender/Desktop/myfile.csv", "a") do |csv|
  a.each do |r|
    b = []
    headers.each do |h|
      b << r.fetch(h)
    end
    csv << b
  end
end

__END__

Handle,Title,Body (HTML),Vendor,Type,Tags,Published,Option1 Name,Option1 Value,Option2 Name,Option2 Value,Option3 Name,Option3 Value,Variant SKU,Variant Grams,Variant Inventory Tracker,Variant Inventory Qty,Variant Inventory Policy,Variant Fulfillment Service,Variant Price,Variant Compare At Price,Variant Requires Shipping,Variant Taxable,Variant Barcode,Image Src,Image Alt Text,Gift Card,SEO Title,SEO Description,Google Shopping / Google Product Category,Google Shopping / Gender,Google Shopping / Age Group,Google Shopping / MPN,Google Shopping / AdWords Grouping,Google Shopping / AdWords Labels,Google Shopping / Condition,Google Shopping / Custom Product,Google Shopping / Custom Label 0,Google Shopping / Custom Label 1,Google Shopping / Custom Label 2,Google Shopping / Custom Label 3,Google Shopping / Custom Label 4,Variant Image,Variant Weight Unit,Variant Tax Code

{:handle=>"yes", :title=>"Yes", :body_html=>"Yes", :vendor=>"piks-s2", :type=>"Shirts", :tags=>"cotton, vintage", :published=>"TRUE", :option1_name=>"Size", :option1_value=>"Small", :option2_name=>"Color", :option2_value=>"blue", :option3_name=>nil, :option3_value=>nil, :variant_sku=>"PIKS-001", :variant_grams=>"100", :variant_inventory_tracker=>"shopify", :variant_inventory_qty=>"20", :variant_inventory_policy=>"continue", :variant_fulfillment_service=>"manual", :variant_price=>"10", :variant_compare_at_price=>"20", :variant_requires_shipping=>"TRUE", :variant_taxable=>"TRUE", :variant_barcode=>nil, :image_src=>"https://cdn.shopify.com/s/files/1/1542/0249/products/200.jpg?v=1476295753", :image_alt_text=>nil, :gift_card=>"FALSE", :seo_title=>nil, :seo_description=>nil, :google_shopping__google_product_category=>nil, :google_shopping__gender=>nil, :google_shopping__age_group=>nil, :google_shopping__mpn=>nil, :google_shopping__adwords_grouping=>nil, :google_shopping__adwords_labels=>nil, :google_shopping__condition=>nil, :google_shopping__custom_product=>nil, :google_shopping__custom_label_0=>nil, :google_shopping__custom_label_1=>nil, :google_shopping__custom_label_2=>nil, :google_shopping__custom_label_3=>nil, :google_shopping__custom_label_4=>nil, :variant_image=>"https://cdn.shopify.com/s/files/1/1542/0249/products/fff.png?v=1476295918", :variant_weight_unit=>"kg", :variant_tax_code=>nil}
