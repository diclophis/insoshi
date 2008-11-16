require 'active_record/connection_adapters/abstract_adapter'
require 'set'

class JsonResult

  attr_accessor :json

  def initialize (json)
    @json = json
  end

  def free()
    #free memory of result table.
    @json = nil
  end

  def data_seek(offset)
    #seek row.
    raise "12"
  end

  def fetch_field()
    #return next Mysql::Field object.
    raise "13"
  end

  def fetch_fields()
    #return Array of Mysql::Field object.
    raise "14"
  end

  def fetch_field_direct(fieldnr)
    #return Mysql::Field object.
    raise "15"
  end

  def fetch_lengths()
    #return Array of field length.
    raise "16"
  end

  def fetch_row()
    #return row as Array.
    raise "1"
  end

  def fetch_hash(with_table=false)
    #return row as Hash. If with_table is true, hash key format is "tablename.fieldname".
    raise "2"
  end

  def field_seek(offset)
    #seek field.
    raise "3"
  end

  def field_tell()
    #return field position.
    raise "4"
  end

  def num_fields()
    #return number of fields.
    raise "5"
  end

  def num_rows()
    #return number of rows.
    raise "6"
  end

  def row_seek(offset)
    #seek row.
    raise "7"
  end

  def row_tell()
    #return row position.
    raise "8"
  end

  def each (&block)
  #raise @json.inspect
    @json["query"]["results"].each { |k, v|
      case k
        when "table"
          v.each { |kk, vv|
            case kk
              when "response"
                vv.each { |kkk, vvv|
                  case kkk
                    when "field"
                      yield vvv
                  else
                    raise "wtf3 #{kkk}"
                  end
                }
              when "name"
              when "sample"
              when "description"
              when "request"
              when "uri"
              when "lang"
              when "created"
              when "count"
              when "updated"
              when "sampleQuery"
            else
              raise "wtf2 #{kk}"
            end
          }
      else
        raise "wtf"
      end
    }
  end

  def all_hashes
    rows = []
    each_hash { |row| rows << row }
    rows
  end

  def each_hash (with_table=false, &block)
    #{|x| ...}
    #'x' is hash of column values, and the keys are the column names.
    @json["query"]["results"].each { |k, v|
      case k
        when "category", "event"
          if v.is_a?(Array) then
            v.each { |vv|
              yield vv
            }
          else
            yield v
          end
      else
        raise "wtf"
      end
    }
  end
end

module ActiveRecord
  class Base
    # Establishes a connection to the database that's used by all Active Record objects.
    def self.yahoo_connection(config) # :nodoc:
      config = config.symbolize_keys
      ConnectionAdapters::YahooAdapter.new(YqlClient.new, logger, [], config)
    end
  end

  module ConnectionAdapters
    class YahooColumn < Column #:nodoc:
      def extract_default(default)
        if type == :binary || type == :text
          if default.blank?
            nil
          else
            raise ArgumentError, "#{type} columns cannot have a default value: #{default.inspect}"
          end
        elsif missing_default_forged_as_empty_string?(default)
          nil
        else
          super
        end
      end

      private
        def simplified_type(field_type)
          return :boolean if YahooAdapter.emulate_booleans && field_type.downcase.index("tinyint(1)")
          return :string  if field_type =~ /enum/i
          super
        end

        def extract_limit(sql_type)
          if sql_type =~ /blob|text/i
            case sql_type
            when /tiny/i
              255
            when /medium/i
              16777215
            when /long/i
              2147483647 # mysql only allows 2^31-1, not 2^32-1, somewhat inconsistently with the tiny/medium/normal cases
            else
              super # we could return 65535 here, but we leave it undecorated by default
            end
          else
            super
          end
        end

        def missing_default_forged_as_empty_string?(default)
          type != :string && !null && default == ''
        end
    end

    class YahooAdapter < AbstractAdapter
      @@emulate_booleans = true
      cattr_accessor :emulate_booleans

      LOST_CONNECTION_ERROR_MESSAGES = [
        "Server shutdown in progress",
        "Broken pipe",
        "Lost connection to MySQL server during query",
        "MySQL server has gone away" ]

      QUOTED_TRUE, QUOTED_FALSE = '1', '0'

      def initialize(connection, logger, connection_options, config)
        super(connection, logger)
        @connection_options, @config = connection_options, config
        @quoted_column_names, @quoted_table_names = {}, {}
        connect
      end

      def adapter_name #:nodoc:
        'Yahoo'
      end

      def supports_migrations? #:nodoc:
        false
      end

      def native_database_types #:nodoc:
        {
          #:primary_key => "int(11) DEFAULT NULL auto_increment PRIMARY KEY",
          :primary_key => "text",
          :string      => { :name => "varchar", :limit => 255 },
          :text        => { :name => "text" },
          :integer     => { :name => "int"},
          :float       => { :name => "float" },
          :decimal     => { :name => "decimal" },
          :datetime    => { :name => "datetime" },
          :timestamp   => { :name => "datetime" },
          :time        => { :name => "time" },
          :date        => { :name => "date" },
          :binary      => { :name => "blob" },
          :boolean     => { :name => "tinyint", :limit => 1 }
        }
      end


      # QUOTING ==================================================

      def quote(value, column = nil)
        if value.kind_of?(String) && column && column.type == :binary && column.class.respond_to?(:string_to_binary)
          s = column.class.string_to_binary(value).unpack("H*")[0]
          "x'#{s}'"
        elsif value.kind_of?(BigDecimal)
          "'#{value.to_s("F")}'"
        else
          super
        end
      end

      def quote_column_name(name) #:nodoc:
        @quoted_column_names[name] ||= name
      end

      def quote_table_name(name) #:nodoc:
        @quoted_table_names[name] ||= name
      end

      def quote_string(string) #:nodoc:
        @connection.quote(string)
      end

      def quoted_true
        QUOTED_TRUE
      end

      def quoted_false
        QUOTED_FALSE
      end

      # CONNECTION MANAGEMENT ====================================
      def active?
        if @connection.respond_to?(:stat)
          @connection.stat
        else
          @connection.query 'show tables'
        end

        @connection.errno.zero?
      rescue Yahoo::Error
        false
      end

      def reconnect!
        disconnect!
        connect
      end

      def disconnect!
        @connection.close rescue nil
      end

      # DATABASE STATEMENTS ======================================
      def select_rows(sql, name = nil)
        result = execute(sql, name)
        rows = []
        result.each { |row| rows << row }
        result.free
        rows
      end

      def execute(sql, name = nil) #:nodoc:
        log(sql, name) { 
          json = @connection.query(sql)
          json_result = JsonResult.new(json)
        }
      rescue ActiveRecord::StatementInvalid => exception
        if exception.message.split(":").first =~ /Packets out of order/
          raise ActiveRecord::StatementInvalid, "'Packets out of order' error was received from the database. Please update your mysql bindings (gem install mysql) and read http://dev.mysql.com/doc/mysql/en/password-hashing.html for more information.  If you're on Windows, use the Instant Rails installer to get the updated mysql bindings."
        else
          raise
        end
      end

      def insert_sql(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil) #:nodoc:
        raise ""
      end

      def update_sql(sql, name = nil) #:nodoc:
        raise ""
      end

      def begin_db_transaction #:nodoc:
        #execute "BEGIN"
      rescue Exception
        # Transactions aren't supported
      end

      def commit_db_transaction #:nodoc:
        #execute "COMMIT"
      rescue Exception
        # Transactions aren't supported
      end

      def rollback_db_transaction #:nodoc:
        #execute "ROLLBACK"
      rescue Exception
        # Transactions aren't supported
      end


      def add_limit_offset!(sql, options) #:nodoc:
        if limit = options[:limit]
          unless offset = options[:offset]
            sql << " LIMIT #{limit}"
          else
            sql << " LIMIT #{limit} OFFSET #{offset}"
          end
        end
      end


      # SCHEMA STATEMENTS ========================================

      def structure_dump #:nodoc:
        sql = "SHOW TABLES"
        select_all(sql).inject("") do |structure, table|
          table.delete('Table_type')
          structure += select_one("SHOW CREATE TABLE #{quote_table_name(table.to_a.first.last)}")["Create Table"] + ";\n\n"
        end
      end

      def recreate_database(name) #:nodoc:
        drop_database(name)
        create_database(name)
      end

      # Create a new MySQL database with optional <tt>:charset</tt> and <tt>:collation</tt>.
      # Charset defaults to utf8.
      #
      # Example:
      #   create_database 'charset_test', :charset => 'latin1', :collation => 'latin1_bin'
      #   create_database 'matt_development'
      #   create_database 'matt_development', :charset => :big5
      def create_database(name, options = {})
        if options[:collation]
          execute "CREATE DATABASE `#{name}` DEFAULT CHARACTER SET `#{options[:charset] || 'utf8'}` COLLATE `#{options[:collation]}`"
        else
          execute "CREATE DATABASE `#{name}` DEFAULT CHARACTER SET `#{options[:charset] || 'utf8'}`"
        end
      end

      def drop_database(name) #:nodoc:
        execute "DROP DATABASE IF EXISTS `#{name}`"
      end

      def current_database
        select_value 'SELECT DATABASE() as db'
      end

      # Returns the database character set.
      def charset
        show_variable 'character_set_database'
      end

      # Returns the database collation strategy.
      def collation
        show_variable 'collation_database'
      end

      def tables(name = nil) #:nodoc:
        tables = []
        execute("SHOW TABLES", name).each { |field| tables << field[0] }
        tables
      end

      def drop_table(table_name, options = {})
        raise ""
      end

      def indexes(table_name, name = nil)
        raise ""
      end

      def columns(table_name, name = nil)#:nodoc:
        sql = "DESC #{quote_table_name(table_name)}"
        columns = []
        execute(sql, name).each { |row| 
        #raise row.inspect
          #row.each { |field|
          #  columns << YahooColumn.new(field["name"], nil, "text", false)
          #}
          row["field"].each { |field|
            columns << YahooColumn.new(field["name"], nil, "text", false)
          }
        }
        columns
      end

      def create_table(table_name, options = {}) #:nodoc:
        raise ""
      end

      def rename_table(table_name, new_name)
        raise ""
      end

      def change_column_default(table_name, column_name, default) #:nodoc:
        raise ""
      end

      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        raise ""
      end

      def rename_column(table_name, column_name, new_column_name) #:nodoc:
        raise ""
      end

      # Maps logical Rails types to MySQL-specific data types.
      def type_to_sql(type, limit = nil, precision = nil, scale = nil)
        return super unless type.to_s == 'integer'

        case limit
        when 0..3
          "smallint(#{limit})"
        when 4..8
          "int(#{limit})"
        when 9..20
          "bigint(#{limit})"
        else
          'int(11)'
        end
      end

      def show_variable(name)
        raise ""
      end

      # Returns a table's primary key and belonging sequence.
      def pk_and_sequence_for(table) #:nodoc:
        keys = []
        execute("describe #{quote_table_name(table)}").each_hash do |h|
          keys << h["Field"]if h["Key"] == "PRI"
        end
        keys.length == 1 ? [keys.first, nil] : nil
      end

      private
        def connect
          @connection.open(*@connection_options)
        end

        def select(sql, name = nil)
          result = execute(sql, name)
          rows = result.all_hashes
          result.free
          rows
        end

        def supports_views?
          false
        end

        def version
          1
        end
    end
  end
end
