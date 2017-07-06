require 'spree_loader_base'
require 'spree_ecom'

module DataShift
  module SpreeEcom
    class ShopifyCustomerLoader

      include DataShift::SpreeLoading

      attr_accessor :file_name

      attr_accessor :datashift_loader

      delegate :loaded_count, :failed_count, :processed_object_count, to: :datashift_loader

      delegate :configure_from, to: :datashift_loader

      # Options
      #
      #  :reload           : Force load of the method dictionary for object_class even if already loaded
      #  :verbose          : Verbose logging and to STDOUT
      #

      def initialize(file_name, options = {})
        add_email_attr_to_address
        @@address_type_id = (options[:address_type].eql?('ship_address')) ? 'ship_address_id' : 'bill_address_id'
        @file_name = file_name
        @options = options

        # gets a file type specific loader e.g csv, excel
        @datashift_loader = DataShift::Loader::Factory.get_loader(file_name)

        #Add this to after setting @datashift_loader
        override_run_method
      end

      def force_inclusion_columns
        @force_inclusion_columns ||= %w{ email
        }
      end

      def run
        logger.info "Users with their address load from File [#{file_name}]"

        DataShift::Configuration.call.mandatory = @options[:mandatory] if @options[:mandatory]
        DataShift::Configuration.call.force_inclusion_of_columns = force_inclusion_columns

        datashift_loader.run(file_name, address_klass, self)
      end

      #override ``save_and_monitor_progress`` method on DocContext object.
      #This method is mainly responsible for saving a record to database.
      #We need to save or create user along with saving address record therefor we need to modify it according to our need.
      def override_save_and_monitor_progress_method(doc_context)

        doc_context.class_eval do

          def save_and_monitor_progress
            if load_object.email.present?
              process_address
            else
              report_failure('Email if empty')
            end
          end

          def process_address
            create_or_fetch_user
            if(errors? && all_or_nothing?)
              # Error already logged with doc_context.failure
              logger.warn "SAVE skipped due to Errors for Row #{ node_context.row_index } - #{ node_context.method_binding.spp }"
            else
              save_address_and_user
            end
          end

          def create_or_fetch_user
            @user = DataShift::SpreeEcom.get_spree_class('User').find_or_create_by!(email: load_object.email) do |u|
              u.password = SecureRandom.hex(16)
            end
          end

          def save_address_and_user
            if save
              report_success
              set_address_for_user
            else
              report_failure
            end
          end

          def report_success
            @progress_monitor.success(load_object)
            logger.info("Successfully Processed [#{node_context.method_binding.spp}]")
            logger.info("Successfully SAVED Object #{@progress_monitor.success_inbound_count} - [#{load_object.id}]")
          end

          def set_address_for_user
            @user.update_attributes!(DataShift::SpreeEcom::ShopifyCustomerLoader.class_variable_get('@@address_type_id') => load_object.reload.id)
          end

          def report_failure(errors = nil)
            failed = FailureData.new(load_object, node_context, errors || current_errors)
            @progress_monitor.failure(failed)
            logger.info("Failed to Process [#{node_context.method_binding.spp}]")
            logger.info("Failed to SAVE Object #{@progress_monitor.success_inbound_count} - [#{load_object.inspect}]")
          end

        end
      end

      private
        #Override ``run`` method LoaderBase class which is accessible through ``@datashift_loader`` (object of CsvLoader Or ExcelLoader).
        #it is done to override ``save_and_monitor_progress`` method of doc_context object
        def override_run_method
          @datashift_loader.class_eval do
            def run(file_name, load_class, customer_loader)
              @file_name = file_name

              setup_load_class(load_class)

              customer_loader.override_save_and_monitor_progress_method(doc_context)

              logger.info("Loading objects of type #{load_object_class}")

              # no implementation - derived classes must implement
              perform_load
            end
          end
        end

        def add_email_attr_to_address
          DataShift::SpreeEcom::get_spree_class('Address').class_eval do
            attr_accessor :email
          end
        end

    end
  end
end
