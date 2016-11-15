require 'spree_base_loader'
require 'spree_ecom'

module DataShift
  module SpreeEcom
    class ShopifyCustomerBinder < Binder
      def forced_inclusion_columns
        super() + ["email"]
      end
    end

		class ShopifyCustomerDocContext < DocContext
      attr_accessor :address_type, :address_type_id

      def initialize(address_type, address_type_id, load_class)
        @address_type = address_type
        @address_type_id = address_type_id

        super(MapperUtils.ensure_class(load_class))
      end

      def save_and_monitor_progress
        begin
          DataShift::SpreeEcom::get_address_class.transaction do
            user_email = @load_object.email
            if(user_email)
              user = DataShift::SpreeEcom::get_user_class.where(email: user_email).first
              if(user)
                logger.info "Skipping User creation as User with email - #{user.email} and id: #{user.id} present"
                if(ship_address = DataShift::SpreeEcom::get_address_class.where(id: user.send(address_type_id)).first)
                  logger.info "Skipping Address creation as User has valid ship address already #{ship_address.inspect}"
                else
                  if(@load_object.save)
                    logger.info "Address created successfully"
                    ship_address = @load_object.reload
                    user.update_attributes!(address_type_id => ship_address.id)
                    logger.info "New Address assigned to user successfully"
                  else
                    logger.error "Address creation failed with error #{@load_object.errors.inspect}"
                  end
                end
              else
                logger.info "Creating User with email #{user_email}"
                if(@load_object.save)
                  logger.info "Address created successfully"
                  ship_address = @load_object.reload
                else
                  logger.error "Address creation failed with error #{@load_object.errors.inspect}"
                  ship_address = nil
                end
                new_user = DataShift::SpreeEcom::get_user_class.create!("email" => user_email, "password" => "vinsol@123", address_type_id => ship_address.try(:id))
                logger.info "New User with email #{user_email} and attributes #{new_user.inspect}"
              end
            else
              failed = FailureData.new(load_object, node_context, current_errors)
              @progress_monitor.failure(failed)

              logger.error "Failed to save row [#{node_context.row_index}] as No Email present"
            end
          end
          logger.info("Successfully SAVED Object with ID #{load_object.id} for Row #{node_context.row_index}")
          @progress_monitor.success(load_object)
        rescue => e
          failed = FailureData.new(load_object, node_context, current_errors)
          @progress_monitor.failure(failed)

          logger.error "Failed to save row [#{node_context.row_index}]"
          logger.error load_object.errors.inspect if(load_object)
          logger.error "Address or User creation failed with error #{e.inspect}"
          logger.error(e.backtrace)
        end
      end
		end

    class ShopifyCustomerLoader < CsvLoader
      @@allowed_address_type = ["ship_address", "bill_address"]
      @@default_address_type = @@allowed_address_type.first

      attr_accessor :address_type, :address_type_id

      def initialize(address_type)
        @address_type = @@allowed_address_type.include?(address_type) ? address_type : @@default_address_type
        @address_type_id = (@address_type == @@default_address_type) ? :ship_address_id : :bill_address_id

        enhance_with_email

        super()

        @binder = ShopifyCustomerBinder.new
      end

      def setup_load_class(load_class)
        @doc_context = ShopifyCustomerDocContext.new(address_type, address_type_id, load_class)
      end

      def perform_load( opts = {} )

        logger.info "Address load from File [#{file_name}]"

        super
      end

      private

        def enhance_with_email
          DataShift::SpreeEcom::get_address_class.class_eval do
            attr_accessor :email
          end
        end
    end
  end
end
