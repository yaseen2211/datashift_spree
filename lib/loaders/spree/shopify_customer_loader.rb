require 'spree_base_loader'
require 'spree_ecom'

module DataShift
  module SpreeEcom
    class ShopifyCustomerLoader < SpreeBaseLoader

      # Options
      #
      #  :reload           : Force load of the method dictionary for object_class even if already loaded
      #  :verbose          : Verbose logging and to STDOUT
      #
      def initialize(address = nil, options = {})
        opts = {:find_operators => true, :instance_methods => true}.merge( options )

        add_email_attr_to_address

        @@allowed_address_type = ["ship_address", "bill_address"]
        @@default_address_type = @@allowed_address_type.first
        @@address_type = @@allowed_address_type.include?(opts[:address_type]) ? opts[:address_type] : @@default_address_type
        @@address_type_id = (@@address_type == @@default_address_type) ? :ship_address_id : :bill_address_id

        p @@address_type_id

        super( DataShift::SpreeEcom::get_address_class, address, opts)

        raise "Failed to create Address for loading" unless @load_object
      end

      # Options:
      #   [:dummy]           : Perform a dummy run - attempt to load everything but then roll back
      #
      def perform_load( file_name, opts = {} )

        logger.info "Address load from File [#{file_name}]"

        options = opts.dup

        # Non Product/database fields we can still process
        @we_can_process_these_anyway =  ["email"]

        if(DataShift::SpreeEcom::version.to_f > 1 )
          options[:force_inclusion] = options[:force_inclusion] ? ([ *options[:force_inclusion]] + @we_can_process_these_anyway) : @we_can_process_these_anyway
        end

        logger.info "Address load using forced operators: [#{options[:force_inclusion]}]" if(options[:force_inclusion])

        super(file_name, options)
      end

      # Over ride base class process with some Spree::Address specifics
      #
      def save_and_report
        begin
          DataShift::SpreeEcom::get_address_class.transaction do
            user_email = @load_object.email
            if(user_email)
              user = DataShift::SpreeEcom::get_user_class.where(email: user_email).first
              if(user)
                logger.info "Skipping User creation as User with email - #{user.email} and id: #{user.id} present"
                if(ship_address = DataShift::SpreeEcom::get_address_class.where(id: user.send(@@address_type_id)).first)
                  logger.info "Skipping Address creation as User has valid ship address already #{ship_address.inspect}"
                else
                  if(@load_object.save)
                    logger.info "Address created successfully"
                    ship_address = @load_object.reload
                    user.update_attributes!(@@address_type_id => ship_address.id)
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
                new_user = DataShift::SpreeEcom::get_user_class.create!("email" => user_email, "password" => SecureRandom.hex(16), @@address_type_id => ship_address.try(:id))
                logger.info "New User with email #{user_email} and attributes #{new_user.inspect}"
              end
            else
              failure
              logger.error "Failed to save row (#{current_row_idx}) - [#{@current_row}] as No Email present"
            end
          end
          logger.info("Successfully SAVED Object with ID #{load_object.id} for Row #{@current_row}")
          @reporter.add_loaded_object(@load_object)
          @reporter.success_inbound_count += 1
        rescue => e
          failure
          logger.error "Failed to save row (#{current_row_idx}) - [#{@current_row}]"
          logger.error load_object.errors.inspect if(load_object)
          logger.error "Address or User creation failed with error #{e.inspect}"
          logger.error(e.backtrace)
        end
      end

      private

        def add_email_attr_to_address
          DataShift::SpreeEcom::get_address_class.class_eval do
            attr_accessor :email
          end
        end

    end
  end
end
