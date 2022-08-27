
module Helpers
  module Config
    def initialize
      @original_config_text = File.read('../../.circleci/pgcat.toml')
      text_to_load = @original_config_text.gsub("5432", "\"5432\"")

      @original_configs = TOML.load(text_to_load)
    end

    def original_configs
      TOML.load(TOML::Generator.new(@original_configs).body)
    end

    def with_modified_configs(new_configs)
      text_to_write = TOML::Generator.new(new_configs).body
      text_to_write = text_to_write.gsub("\"5432\"", "5432")
      File.write('../../.circleci/pgcat.toml', text_to_write)
      yield
    ensure
      File.write('../../.circleci/pgcat.toml', @original_config_text)
    end
  end
end
