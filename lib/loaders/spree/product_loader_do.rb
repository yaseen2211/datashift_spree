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

            parent_taxonomy = DataShift::SpreeEcom::get_spree_class('Taxonomy').where(:name => parent_name).first_or_create

            raise DataShift::DataProcessingError.new("Could not find or create Taxonomy #{parent_name}") unless parent_taxonomy

            parent = parent_taxonomy.root

            # Add the Taxons to Taxonomy from tree structure parent>child>child
            taxons = name_list.collect do |name|

              begin
                taxon = DataShift::SpreeEcom::get_spree_class('Taxon').where(:name => name, :parent_id => parent.id, :taxonomy_id => parent_taxonomy.id).first_or_create

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
    end
  end
end
