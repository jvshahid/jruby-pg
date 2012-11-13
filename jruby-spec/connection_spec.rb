require 'rspec'
require 'spec/lib/helpers'
require 'pg'

describe PG::Connection do
  it 'assumes standard conforming strings is off before any connection is created' do
    # make sure that there are no last connections cached
    PG::Connection.reset_last_conn
    foo = "\x00"
    PG::Connection.escape_bytea(foo).should== "\\\\000"
    @conn = setup_testing_db( "PG_Connection" )
    @conn.exec 'SET standard_conforming_strings = on'
    PG::Connection.escape_bytea(foo).should== "\\000"
    @conn.exec 'SET standard_conforming_strings = off'
    PG::Connection.escape_bytea(foo).should== "\\\\000"
		teardown_testing_db( @conn )
  end

  describe 'prepared statements' do
    before(:all) do
      @conn = setup_testing_db( "PG_Connection" )
    end

    before( :each ) do
      @conn.exec( 'BEGIN' ) unless example.metadata[:without_transaction]
    end

    after( :each ) do
      @conn.exec( 'ROLLBACK' ) unless example.metadata[:without_transaction]
    end

    after(:all) do
      teardown_testing_db( @conn )
    end

    it 'execute successfully' do
      @conn.prepare '', 'SELECT 1 AS n'
      res = @conn.exec_prepared ''
      res[0]['n'].should== '1'
    end

    it 'execute successfully with parameters' do
      @conn.prepare '', 'SELECT $1::text AS n'
      res = @conn.exec_prepared '', ['foo']
      res[0]['n'].should== 'foo'
    end

    it 'should return an error if a prepared statement is used more than once' do
      expect {
        @conn.prepare 'foo', 'SELECT $1::text AS n'
        @conn.prepare 'foo', 'SELECT $1::text AS n'
      }.should raise_error(PGError, /already exists/i)
    end

    it 'return an error if a parameter is not bound to a type' do
      expect {
        @conn.prepare 'bar', 'SELECT $1 AS n'
      }.should raise_error(PGError, /could not determine/i)
    end

    it 'return an error if a prepared statement does not exist' do
      expect {
        @conn.exec_prepared 'foobar'
      }.to raise_error(PGError, /does not exist/i)
    end

    it 'should maintain a correct state after an error' do
      @conn.exec 'ROLLBACK'

      expect {
        res = @conn.exec 'select * from foo'
      }.to raise_error(PGError, /does not exist/)

      expect {
        res = @conn.exec 'SELECT 1 / 0 AS n'
      }.to raise_error(PGError, /by zero/)
    end

    it 'should correctly accept queries after a query is cancelled' do
      @conn.exec 'ROLLBACK'
      @conn.send_query 'SELECT pg_sleep(1000)'
      @conn.cancel
      res = @conn.get_result
      @conn.exec 'select pg_sleep(1)'
    end

    it 'exec should clear results from previous queries' do
      @conn.exec 'ROLLBACK'
      @conn.send_query 'SELECT pg_sleep(1000)'
      @conn.cancel
      @conn.block
      @conn.exec 'ROLLBACK'
    end

    # FIXME: how does this spec pass in ruby-pg without the last get_last_result
    # not calling get_last_reuslt will leave the connection in a state that
    # doesn't accept new queries and ROLLBACK will fail
    it "described_class#block should allow a timeout" do
      @conn.send_query( "select pg_sleep(3)" )

      start = Time.now
      @conn.block( 0.1 )
      finish = Time.now

      (finish - start).should be_within( 0.05 ).of( 0.1 )
    end

    it 'correctly translates the server version' do
      @conn.server_version.should >=(80200)
    end

    it 'quotes identifier correctly' do
      table_name = @conn.quote_ident('foo')
      column_name = @conn.quote_ident('bar')
      @conn.exec "CREATE TABLE #{table_name} (#{column_name} text)"
    end

    it 'quotes identifier correctly when the static quote_ident is called' do
      table_name = PG::Connection.quote_ident('foo')
      column_name = PG::Connection.quote_ident('bar')
      @conn.exec "CREATE TABLE #{table_name} (#{column_name} text)"
    end

    it 'returns an empty result set when an INSERT is executed' do
      res = @conn.exec 'CREATE TABLE foo (bar INT)'
      res.should_not be_nil
      res = @conn.exec 'INSERT INTO foo VALUES (1234)'
      res.should_not be_nil
      res.nfields.should ==(0)
    end

    it 'delete does not fail' do
      @conn.exec 'CREATE TABLE foo (bar INT)'
      @conn.exec 'INSERT INTO foo VALUES (1234)'
      res = @conn.exec 'DELETE FROM foo WHERE bar = 1234'
      res.should_not be_nil
      res.nfields.should ==(0)
    end

    it 'returns id when a new row is inserted' do
      @conn.exec 'CREATE TABLE foo (id SERIAL UNIQUE, bar INT)'
      @conn.prepare 'query', 'INSERT INTO foo(bar) VALUES ($1) returning id'
      @conn.send_query_prepared 'query', ['1234']
      @conn.block
      res = @conn.get_last_result
      res.should_not be_nil
      res.nfields.should ==(1)
    end

    it "correctly finishes COPY queries passed to #async_exec" # do
    # 	@conn.async_exec( "COPY (SELECT 1 UNION ALL SELECT 2) TO STDOUT" )

    # 	results = []
    # 	begin
    # 		data = @conn.get_copy_data( true )
    # 		if false == data
    # 			@conn.block( 2.0 )
    # 			data = @conn.get_copy_data( true )
    # 		end
    # 		results << data if data
    # 	end until data.nil?

    # 	results.should have( 2 ).members
    # 	results.should include( "1\n", "2\n" )
    # end

    # large object api
    # notifications
  end
end
