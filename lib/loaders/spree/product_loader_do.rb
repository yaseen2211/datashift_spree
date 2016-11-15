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
                  if(value && model_method.operator?('taxons'))
                    add_taxons(value, doc_context, binder)
                  elsif(value && model_method.operator?('product_properties'))
                    add_properties(value, doc_context, binder)
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

      def get_each_assoc(value, binder)
        value.to_s.split( binder.multi_assoc_delim )
      end

      def get_each_val(value, binder)
        value.to_s.split( binder.name_value_delim )
      end

      private

        def add_taxons(value, doc_context, binder)
          # TODO smart column ordering to ensure always valid by time we get to associations
          doc_context.save_if_new
          load_object = doc_context.load_object

          chain_list = get_each_assoc(value, binder)  # potentially multiple chains in single column (delimited by Delimiters::multi_assoc_delim)

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
      end
    end
  end
end
