require_relative 'spec_helper'


describe "Plugins" do
  let(:processes) { Helpers::Pgcat.three_shard_setup("sharded_db", 5) }

  context "intercept" do
    it "will intercept an intellij query" do
      conn = PG.connect(processes.pgcat.connection_string("sharded_db", "sharding_user"))
      res = conn.exec("select current_database() as a, current_schemas(false) as b")
      expect(res.values).to eq([["sharded_db", "{public}"]])
    end
  end
end
