require 'rspec'
require 'spec/lib/helpers'
require 'pg'

describe PG::Connection do
  it 'assumes standard conforming strings is off before any connection is created' do
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
  end
end
