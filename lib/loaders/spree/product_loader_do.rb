# Copyright:: (c) Autotelik Media Ltd 2010
# Author ::   Tom Statter
# Date ::     Aug 2010
# License::   MIT ?
#
# Details::   Specific over-rides/additions to support Spree Products
#
require 'spree_base_loader'
require 'spree_ecom'

module DataShift
  module SpreeEcom
    class ShopifyProductBinder < Binder
      def forced_inclusion_columns
        extra_cols = ["images","variant_sku","variant_cost_price","variant_price","variant_images","stock_items", "price"]
        super() + extra_cols
      end
    end

    class ShopifyProductLoader < CsvLoader
      def initialize
        @@image_klass ||= DataShift::SpreeEcom::get_spree_class('Image')
        @@option_type_klass ||= DataShift::SpreeEcom::get_spree_class('OptionType')
        @@option_value_klass ||= DataShift::SpreeEcom::get_spree_class('OptionValue')
        @@product_klass ||= DataShift::SpreeEcom::get_spree_class('Product')
        @@property_klass ||= DataShift::SpreeEcom::get_spree_class('Property')
        @@product_property_klass ||= DataShift::SpreeEcom::get_spree_class('ProductProperty')
        @@stock_location_klass ||= DataShift::SpreeEcom::get_spree_class('StockLocation')
        @@stock_movement_klass ||= DataShift::SpreeEcom::get_spree_class('StockMovement')
        @@taxonomy_klass ||= DataShift::SpreeEcom::get_spree_class('Taxonomy')
        @@taxon_klass ||= DataShift::SpreeEcom::get_spree_class('Taxon')
        @@variant_klass ||= DataShift::SpreeEcom::get_spree_class('Variant')

        super
      end

			def perform_load( _options = {} )
				require 'csv'

				raise "Cannot load - failed to create a #{klass}" unless load_object

				logger.info "Starting bulk load from CSV : #{file_name}"

				# TODO: - can we abstract out what a 'parsed file' is - headers plus value of each node
				# so a common object can represent excel,csv etc
				# then  we can make load() more generic

				parsed_file = CSV.read(file_name)

				# assume headers are row 0
				header_idx = 0
				header_row = parsed_file.shift

				set_headers( DataShift::Headers.new(:csv, header_idx, header_row) )

				# maps list of headers into suitable calls on the Active Record class
				bind_headers(headers)

				begin
					puts 'Dummy Run - Changes will be rolled back' if(configuration.dummy_run)

					load_object_class.transaction do
						logger.info "Processing #{parsed_file.size} rows"

						parsed_file.each_with_index do |row, j|

							logger.info "Processing Row #{j} : #{row}"

							# Iterate over the bindings, creating a context from data in associated Excel column

							@binder.bindings.each_with_index do |method_binding, i|

								unless method_binding.valid?
									logger.warn("No binding was found for column (#{i}) [#{method_binding.pp}]")
									next
								end

								# If binding to a column, get the value from the cell (bindings can be to internal methods)
								value = method_binding.index ? row[method_binding.index] : nil

								context = doc_context.create_node_context(method_binding, i, value)

								logger.info "Processing Column #{method_binding.index} (#{method_binding.pp})"

                model_method = method_binding.model_method

								begin
                  if(value && model_method.operator?('variants'))
                    add_options_variants(value, doc_context, binder)
                  elsif(value && model_method.operator?('taxons'))
                    add_taxons(value, doc_context, binder)
                  elsif(value && model_method.operator?('product_properties'))
                    add_properties(value, doc_context, binder)
                  elsif(value && model_method.operator?('stock_items'))
                    add_variants_stock(value, doc_context, binder)
                  else
                    context.process
                  end
								rescue => x
									if doc_context.all_or_nothing?
										logger.error('Complete Row aborted - All or nothing set and Current Column failed.')
										logger.error(x.backtrace.first.inspect)
										logger.error(x.inspect)
										break
									end
								end
							end # end of each column(node)

							doc_context.save_and_monitor_progress

							doc_context.reset unless doc_context.node_context.next_update?
						end # all rows processed

						if(configuration.dummy_run)
							puts 'CSV loading stage done - Dummy run so Rolling Back.'
							raise ActiveRecord::Rollback # Don't actually create/upload to DB if we are doing dummy run
						end
					end # TRANSACTION N.B ActiveRecord::Rollback does not propagate outside of the containing transaction block

				rescue => e
					puts "ERROR: CSV loading failed : #{e.inspect}"
					raise e
				ensure
					report
				end

				puts 'CSV loading stage Complete.'
			end

      private

        def get_each_assoc(value, binder)
          value.to_s.split( binder.multi_assoc_delim )
        end

        def get_each_val(value, binder)
          value.to_s.split( binder.name_value_delim )
        end

        def add_taxons(value, doc_context, binder)
          # TODO smart column ordering to ensure always valid by time we get to associations
          doc_context.save_if_new
          load_object = doc_context.load_object

          chain_list = get_each_assoc(value, binder)  # potentially multiple chains in single column (delimited by binder.multi_assoc_delim)

          chain_list.each do |chain|

            # Each chain can contain either a single Taxon, or the tree like structure parent>child>child
            name_list = chain.split(/\s*>\s*/)

            parent_name = name_list.shift

            parent_taxonomy = @@taxonomy_klass.where(:name => parent_name).first_or_create

            raise DataShift::DataProcessingError.new("Could not find or create Taxonomy #{parent_name}") unless parent_taxonomy

            parent = parent_taxonomy.root

            # Add the Taxons to Taxonomy from tree structure parent>child>child
            taxons = name_list.collect do |name|

              begin
                taxon = @@taxon_klass.where(:name => name, :parent_id => parent.id, :taxonomy_id => parent_taxonomy.id).first_or_create

                # pre Rails 4 -  taxon = @@taxon_klass.find_or_create_by_name_and_parent_id_and_taxonomy_id(name, parent && parent.id, parent_taxonomy.id)

                unless(taxon)
                  logger.warn("Missing Taxon - could not find or create #{name} for parent #{parent_taxonomy.inspect}")
                end
              rescue => e
                logger.error(e.inspect)
                logger.error "Cannot assign Taxon ['#{taxon}'] to Product ['#{load_object.name}']"
                next
              end

              parent = taxon  # current taxon becomes next parent
              taxon
            end

            taxons << parent_taxonomy.root

            unique_list = taxons.compact.uniq - (load_object.taxons || [])

            logger.debug("Product assigned to Taxons : #{unique_list.collect(&:name).inspect}")

            load_object.taxons << unique_list unless(unique_list.empty?)
            # puts load_object.taxons.inspect

          end
        end

        # Special case for ProductProperties since it can have additional value applied.
        # A list of Properties with a optional Value - supplied in form :
        #   property_name:value|property_name|property_name:value
        #  Example :
        #  test_pp_002|test_pp_003:Example free value|yet_another_property

        def add_properties(value, doc_context, binder)
          # TODO smart column ordering to ensure always valid by time we get to associations
          doc_context.save_if_new
          load_object = doc_context.load_object

          property_list = get_each_assoc(value, binder)

          property_list.each do |pstr|

            # Special case, we know we lookup on name so operator is effectively the name to lookup
            find_by_name, find_by_value = get_each_val( pstr, binder )

            raise "Cannot find Property via #{find_by_name} (with value #{find_by_value})" unless(find_by_name)

            property = @@property_klass.where(:name => find_by_name).first

            unless property
              property = @@property_klass.create( :name => find_by_name, :presentation => find_by_name.humanize)
              logger.info "Created New Property #{property.inspect}"
            end

            if(property)
                # Property now protected from mass assignment
                x = @@product_property_klass.new( :value => find_by_value )
                x.property = property
                x.save
                load_object.product_properties << x
                logger.info "Created New ProductProperty #{x.inspect}"
            else
              puts "WARNING: Property #{find_by_name} NOT found - Not set Product"
            end
          end
        end

        def add_variants_stock(value, doc_context, binder)
          # TODO smart column ordering to ensure always valid by time we get to associations
          doc_context.save_if_new
          load_object = doc_context.load_object

          # do we have Variants?
          if(load_object.variants.size > 0)

            logger.info "[COUNT_ON_HAND] - number of variants to process #{load_object.variants.size}"

            if(value.to_s.include?(binder.multi_assoc_delim))
              # Check if we've already processed Variants and assign count per variant
              values = value.to_s.split(binder.multi_assoc_delim)
              # variants and count_on_hand number match?
              raise "WARNING: Count on hand entries did not match number of Variants - None Set" unless (load_object.variants.size == values.size)
            end

            variants = load_object.variants # just for readability and logic
            logger.info "Variants: #{load_object.variants.inspect}"

            stock_coh_list = get_each_assoc(value, binder) # we expect to get corresponding stock_location:count_on_hand for every variant

            stock_coh_list.each_with_index do |stock_coh, i|

              # count_on_hand column MUST HAVE "stock_location_name:variant_count_on_hand" format
              if(stock_coh.to_s.include?(binder.name_value_delim))
                stock_location_name, variant_count_on_hand = stock_coh.split(binder.name_value_delim)
              else
                stock_location_name, variant_count_on_hand = [nil, stock_coh]
              end

              logger.info "Setting #{variant_count_on_hand} items for stock location #{stock_location_name}..."

              if not stock_location_name # No Stock Location referenced, fallback to default one...
                logger.info "No Stock Location was referenced. Adding count_on_hand to default Stock Location. Use 'stock_location_name:variant_count_on_hand' format to specify prefered Stock Location"
                stock_location = @@stock_location_klass.where(:default => true).first
                raise "WARNING: Can't set count_on_hand as no Stock Location exists!" unless stock_location
              else # go with the one specified...
                stock_location = @@stock_location_klass.where(:name => stock_location_name).first
                unless stock_location
                  stock_location = @@stock_location_klass.create( :name => stock_location_name)
                  logger.info "Created New Stock Location #{stock_location.inspect}"
                end
              end

              if(stock_location)
                  @@stock_movement_klass.create(:quantity => variant_count_on_hand.to_i, :stock_item => variants[i].stock_items.find_by_stock_location_id(stock_location.id))
                  logger.info "Added #{variant_count_on_hand} count_on_hand to Stock Location #{stock_location.inspect}"
              else
                puts "WARNING: Stock Location #{stock_location_name} NOT found - Can't set count_on_hand"
              end

            end

          # ... or just single Master Product?
          elsif(load_object.variants.size == 0)
            if(value.to_s.include?(binder.multi_assoc_delim))
              # count_on_hand column MUST HAVE "stock_location_name:master_count_on_hand" format
              stock_location_name, master_count_on_hand = (value.to_s.split(binder.multi_assoc_delim).first).split(binder.name_value_delim)
              puts "WARNING: Multiple count_on_hand values specified but no Variants/OptionTypes created"
            else
              if(current_value.to_s.include?(binder.name_value_delim))
                stock_location_name, master_count_on_hand = current_value.split(binder.name_value_delim)
              else
                stock_location_name, master_count_on_hand = [nil, current_value]
              end
            end
            if not stock_location_name # No Stock Location referenced, fallback to default one...
              logger.info "No Stock Location was referenced. Adding count_on_hand to default Stock Location. Use 'stock_location_name:master_count_on_hand' format to specify prefered Stock Location"
              stock_location = @@stock_location_klass.where(:default => true).first
              raise "WARNING: Can't set count_on_hand as no Stock Location exists!" unless stock_location
            else # go with the one specified...
              stock_location = @@stock_location_klass.where(:name => stock_location_name).first
              unless stock_location
                stock_location = @@stock_location_klass.create( :name => stock_location_name)
                logger.info "Created New Stock Location #{stock_location.inspect}"
              end
            end

            if(stock_location)
                @@stock_movement_klass.create(:quantity => master_count_on_hand.to_i, :stock_item => load_object.master.stock_items.find_by_stock_location_id(stock_location.id))
                logger.info "Added #{master_count_on_hand} count_on_hand to Stock Location #{stock_location.inspect}"
            else
              puts "WARNING: Stock Location #{stock_location_name} NOT found - Can't set count_on_hand"
            end
          end
        end

        # Special case for OptionTypes as it's two stage process
        # First add the possible option_types to Product, then we are able
        # to define Variants on those options values.
        # So to define a Variant :
        #   1) define at least one OptionType on Product, for example Size
        #   2) Provide a value for at least one of these OptionType
        #   3) A composite Variant can be created by supplying a value for more than one OptionType
        #       fro example Colour : Red and Size Medium
        # Supported Syntax :
        #  '|' seperates Variants
        #
        #   ';' list of option values
        #  Examples : 
        #  
        #     mime_type:jpeg;print_type:black_white|mime_type:jpeg|mime_type:png, PDF;print_type:colour
        #
        def add_options_variants(value, doc_context, binder)
          doc_context.save_if_new
          load_object = doc_context.load_object

          # TODO smart column ordering to ensure always valid by time we get to associations
          # example : mime_type:jpeg;print_type:black_white|mime_type:jpeg|mime_type:png, PDF;print_type:colour

          variants = get_each_assoc(value, binder)  # potentially multiple chains in single column (delimited by binder.multi_assoc_delim)

          logger.info "Adding Options Variants #{variants.inspect}"

          # example line becomes :  
          #   1) mime_type:jpeg|print_type:black_white  
          #   2) mime_type:jpeg  
          #   3) mime_type:png, PDF|print_type:colour

          variants.each do |per_variant|

            option_types = per_variant.split(binder.multi_facet_delim)    # => [mime_type:jpeg, print_type:black_white]

            logger.info "Checking Option Types #{option_types.inspect}"

            optiontype_vlist_map = {}

            option_types.each do |ostr|

              oname, value_str = ostr.split(binder.name_value_delim)

              option_type = @@option_type_klass.where(:name => oname).first

              unless option_type
                option_type = @@option_type_klass.create(:name => oname, :presentation => oname.humanize)
                # TODO - dynamic creation should be an option

                unless option_type
                  logger.warm("WARNING: OptionType #{oname} NOT found and could not create - Not set Product")
                  next
                end
                logger.info "Created missing OptionType #{option_type.inspect}"
              end

              # OptionTypes must be specified first on Product to enable Variants to be created
              load_object.option_types << option_type unless load_object.option_types.include?(option_type)

              # Can be simply list of OptionTypes, some or all without values
              next unless(value_str)

              optiontype_vlist_map[option_type] ||= []

              # Now get the value(s) for the option e.g red,blue,green for OptType 'colour'
              optiontype_vlist_map[option_type] += value_str.split(',').flatten

              logger.debug("Parsed OptionValues #{optiontype_vlist_map[option_type]} for Option_Type #{option_type.name}")
            end

            next if(optiontype_vlist_map.empty?) # only option types specified - no values

            # Now create set of Variants, some of which maybe composites
            # Find the longest set of OptionValues to use as base for combining with the rest
            sorted_map = optiontype_vlist_map.sort_by { |ot, ov| ov.size }.reverse

            logger.debug("Processing Options into Variants #{sorted_map.inspect}")

            # {mime => ['pdf', 'jpeg', 'gif'], print_type => ['black_white']}

            lead_option_type, lead_ovalues = sorted_map.shift

            # TODO .. benchmarking to find most efficient way to create these but ensure Product.variants list
            # populated .. currently need to call reload to ensure this (seems reqd for Spree 1/Rails 3, wasn't required b4
            lead_ovalues.each do |ovname|

              ov_list = []

              ovname.strip!

              #TODO - not sure why I create the OptionValues here, rather than above with the OptionTypes
              ov = @@option_value_klass.where(:name => ovname, :option_type_id => lead_option_type.id).first_or_create(:presentation => ovname.humanize)
              ov_list << ov if ov

              # Process rest of array of types => values
              sorted_map.each do |ot, ovlist| 
                ovlist.each do |ov_for_composite|

                  ov_for_composite.strip!

                  # Prior Rails 4 - ov = @@option_value_klass.find_or_create_by_name_and_option_type_id(for_composite, ot.id, :presentation => for_composite.humanize)
                  ov = @@option_value_klass.where(:name => ov_for_composite, :option_type_id => ot.id).first_or_create(:presentation => ov_for_composite.humanize)

                  ov_list << ov if(ov)
                end
              end

              unless(ov_list.empty?)

                logger.info("Creating Variant from OptionValue(s) #{ov_list.collect(&:name).inspect}")

                i = load_object.variants.size + 1

                variant = load_object.variants.create( :sku => "#{load_object.sku}_#{i}", :price => load_object.price, :weight => load_object.weight, :height => load_object.height, :width => load_object.width, :depth => load_object.depth, :tax_category_id => load_object.tax_category_id)

                variant.option_values << ov_list if(variant)
              end
            end

            load_object.reload unless load_object.new_record?
            #puts "DEBUG Load Object now has Variants : #{load_object.variants.inspect}" if(verbose)
          end
        end # each Variant
    end
  end
end
