# Copyright:: (c) Autotelik Media Ltd 2011
# Author ::   Tom Statter
# Date ::     Jan 2011
# License::   MIT. Free, Open Source.
#
require 'loader_base'
require 'spree_base_loader'
#require 'paperclip/attachment_loader'

module DataShift
  module SpreeEcom
    class ImageBinder < Binder
      def include_all?
        true
      end
    end

    # Very specific Image Loading for existing Products in Spree. 
    #
    # Requirements : A CSV or Excel file which has 2+ columns
    # 
    #   1)  Identifies a Product via Name or SKU column
    #   2+) The full path(s) to the Images to attach to Product from column 1
    #
    class MigrateImageLoader < CsvLoader
      include DataShift::ImageLoading

      attr_accessor :image_path_prefix

      def initialize(opts = {})
        @image_path_prefix = opts[:image_path_prefix]
        @@image_klass ||= DataShift::SpreeEcom::get_spree_class('Image')
        @@product_klass ||= DataShift::SpreeEcom::get_spree_class('Product')
        @@variant_klass ||= DataShift::SpreeEcom::get_spree_class('Variant')
        @@product_model_methods = ModelMethods::Manager.catalog_class(@@product_klass)
        @@variant_model_methods = ModelMethods::Manager.catalog_class(@@variant_klass)
        @@path_headers ||= ['attachment', 'images', 'path']

        super()

        @binder = ImageBinder.new
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
              doc_context.load_object = nil

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
                operator = model_method.operator

								begin
                  if(value && @@path_headers.include?(operator))
                    p "Image Path"
                    add_images(value, doc_context, binder) if doc_context.load_object
                  elsif(value && operator)
                    if @@product_model_methods.search(operator)
                      p "Product Matcher"
                      doc_context.load_object = get_record_by(@@product_klass, operator, value)
                    elsif @@variant_model_methods.search(operator)
                      p "Variant Matcher"
                      doc_context.load_object = get_record_by(@@variant_klass, operator, value)
                    else
                      raise "No Spree class can be searched for by #{operator}"
                    end
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

              if(doc_context.load_object.nil?)
                puts "WARNING: Could not find a record where #{row[0]}"
                doc_context.progress_monitor.failure(DataShift::FailureData.new(nil, nil, []))
              else
                puts "Image Attachment on record #{doc_context.load_object.inspect}"
                doc_context.progress_monitor.success(doc_context.load_object)
              end

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

        def add_images(value, doc_context, binder)
          load_object = doc_context.load_object

          ## Needed in finding by images, adding kind a mock here
          config = {image_path_prefix: image_path_prefix}

          # different versions have moved images around from Prod to Variant
          owner = DataShift::SpreeEcom::get_image_owner(load_object)
          p owner

          all_images = get_each_assoc(value, binder)

          # multiple files should maintain comma separated logic with 'binder.multi_value_delim' and not 'binder.multi_assoc_delim'
          all_images.each do |image|

            #TODO - make this binder.attributes_start_delim and support {alt=> 'blah, :position => 2 etc}

            # Test and code for this saved at : http://www.rubular.com/r/1de2TZsVJz

            @spree_uri_regexp ||= Regexp::new('(http|ftp|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:\/~\+#]*[\w\-\@?^=%&amp;\/~\+#])?' )

            if(image.match(@spree_uri_regexp))

              uri, attributes = image.split(binder.attribute_list_start)

              uri.strip!

              logger.info("Processing IMAGE from URI [#{uri.inspect}]")

              if(attributes)
                #TODO move to ColumnPacker unpack ?
                attributes = attributes.split(', ').map{|h| h1,h2 = h.split('=>'); {h1.strip! => h2.strip!}}.reduce(:merge)
                logger.debug("IMAGE has additional attributes #{attributes.inspect}")
              else
                attributes = {} # will blow things up later if we pass nil where {} expected
              end

              agent = Mechanize.new

              image = begin
                agent.get(uri)
              rescue => e
                puts "ERROR: Failed to fetch image from URL #{uri}", e.message
                raise DataShift::BadUri.new("Failed to fetch image from URL #{uri}")
              end

              # Expected image is_a Mechanize::Image
              # image.filename& image.extract_filename do not handle query string well e,g blah.jpg?v=1234
              # so for now use URI
              # extname = image.respond_to?(:filename) ? File.extname(image.filename) : File.extname(uri)
              extname =  File.extname( uri.gsub(/\?.*=.*/, ''))

              base = image.respond_to?(:filename) ? File.basename(image.filename, '.*') : File.basename(uri, '.*')

              logger.debug("Storing Image in TempFile #{base.inspect}.#{extname.inspect}")

              @current_image_temp_file = Tempfile.new([base, extname], :encoding => 'ascii-8bit')

              begin

                # TODO can we handle embedded img src e.g from Mechanize::Page::Image ?

                # If I call image.save(@current_image_temp_file.path) then it creates a new file with a .1 extension
                # so the real temp file data is empty and paperclip chokes
                # so this is a copy from the Mechanize::Image save method.  don't like it much, very brittle, but what to do ...
                until image.body_io.eof? do
                  @current_image_temp_file.write image.body_io.read 16384
                end

                @current_image_temp_file.rewind

                logger.info("IMAGE downloaded from URI #{uri.inspect}")

                attachment = create_attachment(@@image_klass, @current_image_temp_file.path, nil, nil, attributes)

              rescue => e
                logger.error(e.message)
                logger.error("Failed to create Image from URL #{uri}")
                raise DataShift::DataProcessingError.new("Failed to create Image from URL #{uri}")

              ensure
                @current_image_temp_file.close
                @current_image_temp_file.unlink
              end

            else

              path, alt_text = image.split(binder.name_value_delim)

              logger.debug("Processing IMAGE from PATH #{path.inspect} #{alt_text.inspect}")

              path = File.join(config[:image_path_prefix], path) if(config[:image_path_prefix])

              attachment = create_attachment(@@image_klass, path, nil, nil, :alt => alt_text)
            end

            begin
              owner.images << attachment

              logger.debug("Product assigned Image from : #{path.inspect}")
            rescue => e
              p e.message
              puts "ERROR - Failed to assign attachment to #{owner.class} #{owner.id}"
              logger.error("Failed to assign attachment to #{owner.class} #{owner.id}")
            end

          end

          load_object.save

        end
    end
  end
end
