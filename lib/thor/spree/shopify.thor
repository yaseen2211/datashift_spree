# Copyright:: (c) Autotelik Media Ltd 2015
# Author ::   Tom Statter
# Date ::     March 2015
# License::   MIT. Free, Open Source.
#
# Note, not DataShift, case sensitive, create namespace for command line : datashift

require 'datashift_spree'
require 'spree_ecom'

module DatashiftSpree

  class Shopify < Thor

    include DataShift::Logging


    desc "users", "Populate Spree User data from Shopify CSV file"

    method_option :input, :aliases => '-i', :required => true, :desc => "The .csv import file"
    method_option :config, :aliases => '-c',  :type => :string, :desc => "Configuration file containg defaults or over rides in YAML"
    method_option :dummy, :aliases => '-d', :type => :boolean, :desc => "Dummy run, do not actually save Image or Product"
    method_option :verbose, :aliases => '-v', :type => :boolean, :desc => "Verbose logging"
    method_option :address_type, :aliases => '-a', :type => :string, :default => "ship_address", :desc => "Ship or Bill Address Type"

    def users()
      # Address class as user is imported along with address
      require File.expand_path('config/environment.rb')

      user_import(importer, options)
    end

    desc "orders", "Populate Spree Order data from Shopify CSV file"

    method_option :input, :aliases => '-i', :required => true, :desc => "The .csv import file"
    method_option :verbose, :aliases => '-v', :type => :boolean, :desc => "Verbose logging"
    method_option :config, :aliases => '-c',  :type => :string, :desc => "Configuration file containg defaults or over rides in YAML"
    method_option :dummy, :aliases => '-d', :type => :boolean, :desc => "Dummy run, do not actually save Image or Product"

    def orders()

      # assuming run from a rails app/top level dir
      require File.expand_path('config/environment.rb')

      # Use default logging formatter so that PID and timestamp are not suppressed.
      Rails.application.config.log_formatter = ::Logger::Formatter.new

      order_import(options)
    end

    no_commands do
      def start_connections
        if File.exist?(File.expand_path('config/environment.rb'))
          begin
            require File.expand_path('config/environment.rb')
          rescue => e
            logger.error("Failed to initialise ActiveRecord : #{e.message}")
            raise DataShift::ConnectionError.new("Failed to initialise ActiveRecord : #{e.message}")
          end
        else
          raise DataShift::PathError.new('No config/environment.rb found - cannot initialise ActiveRecord')
        end
      end

      def user_import(options)
        importer = DataShift::SpreeEcom::ShopifyCustomerLoader.new(options["address_type"])

        logger.info "Datashift: Starting User Import from #{options[:input]}"

        importer.configure_from( options[:config] ) if(options[:config])

        importer.run(options[:input], DataShift::SpreeEcom.get_address_class)
      end

      def order_import(options)
        importer = DataShift::SpreeEcom::ShopifyOrderLoader.new

        logger.info "Datashift: Starting Order Import from #{options[:input]}"

        importer.configure_from( options[:config] ) if(options[:config])

        importer.run(options[:input], DataShift::SpreeEcom.get_order_class)
      end
    end
  end
end
