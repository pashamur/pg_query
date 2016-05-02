require 'spec_helper'

def filter_columns(qstr)
  q = PgQuery.parse(qstr)
  q.filter_columns
end

def simple_where_conditions(qstr)
  q = PgQuery.parse(qstr)
  q.simple_where_conditions
end

describe PgQuery, '#filter_columns' do
  it 'finds unqualified names' do
    expect(filter_columns('SELECT * FROM x WHERE y = ? AND z = 1')).to eq [[nil, 'y'], [nil, 'z']]
  end

  it 'finds qualified names' do
    expect(filter_columns('SELECT * FROM x WHERE x.y = ? AND x.z = 1')).to eq [['x', 'y'], ['x', 'z']]
  end

  it 'traverses into CTEs' do
    query = 'WITH a AS (SELECT * FROM x WHERE x.y = ? AND x.z = 1) SELECT * FROM a WHERE b = 5'
    expect(filter_columns(query)).to match_array [['x', 'y'], ['x', 'z'], [nil, 'b']]
  end

  it 'is able to extract simple integer where conditions' do
    expect(simple_where_conditions('SELECT * FROM x WHERE y = 1 AND z = 2')).to eq [['y', '=', 1], ['z', '=', 2]]
  end

  it 'is able to extract simple string where conditions' do
    expect(simple_where_conditions("SELECT * FROM x WHERE y > '1' AND z ~* 'small'")).to eq [['y', '>', '1'], ['z', '~*', 'small']]
  end

  it 'is able to extract simple where conditions with table' do
    expect(simple_where_conditions("SELECT * FROM x WHERE x.y = 'test' AND z >= 53")).to eq [['x.y', '=', 'test'], ['z', '>=', 53]]
  end

  it 'is able to extract simple where conditions with table' do
    expect(simple_where_conditions("SELECT * FROM x WHERE x.y = 'test' AND z >= 53")).to eq [['x.y', '=', 'test'], ['z', '>=', 53]]
  end

  it 'is able to parse contions out of CTEs' do
    query = 'WITH a AS (SELECT * FROM x WHERE x.y = 10 AND x.z = 1) SELECT * FROM a WHERE b = 5'
    expect(simple_where_conditions(query)).to eq [['b', '=', 5], ['x.y', '=', 10], ['x.z', '=', 1]]
  end

  it 'is able to parse floats and BETWEEN expressions' do
    query = "SELECT * FROM x WHERE x.y = 5.3 AND z BETWEEN 5 AND 10"
    expect(simple_where_conditions(query)).to eq [['x.y', '=', 5.3], ['z', '>=', 5], ['z', '<=', 10]]
  end

  it 'ignores typecasts' do
    query = "SELECT * FROM x WHERE created_at > '2016-04-01'::date AND z = 10"
    expect(simple_where_conditions(query)).to eq [['z', '=', 10]]
  end

  it 'is able to parse IS (T/F) and IS NOT (T/F)' do
    query = "SELECT * FROM x WHERE a IS TRUE and b IS NOT FALSE"
    expect(simple_where_conditions(query)).to eq [['a', 'IS', true], ['b', 'IS NOT', false]]
  end

  it 'is able to parse null tests' do
    query = "SELECT * FROM x WHERE a IS NULL and b IS NOT NULL"
    expect(simple_where_conditions(query)).to eq [['a', 'IS', 'NULL'], ['b', 'IS NOT', 'NULL']]
  end

  it 'is able to parse integer IN conditions' do
    query = "SELECT * FROM x WHERE a IN (1,2,3)"
    expect(simple_where_conditions(query)).to eq [['a', 'IN', '(1,2,3)']]
  end

  it 'is able to parse string IN conditions' do
    query = "SELECT * FROM x WHERE a IN ('a','b','c')"
    expect(simple_where_conditions(query)).to eq [['a', 'IN', "('a','b','c')"]]
  end

end
