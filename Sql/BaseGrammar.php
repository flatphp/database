<?php namespace Flatphp\Database\Sql;

class BaseGrammar
{
    /**
     * @var Builder
     */
    protected $_builder;
    protected $_select = array(
        'select',
        'joins',
        'wheres',
        'group', 'having',
        'orders',
        'limit', 'offset'
    );

    public function getSelect(Builder $builder)
    {
        $this->_builder = $builder;
        $sql = [];
        foreach ($this->_select as $item) {
            if (!is_null($this->_builder->$item)) {
                $method = '_asbl'. ucfirst($item);
                $sql[] = $this->$method();
            }
        }
        return implode(' ', $sql);
    }

    public function getInsert(Builder $builder)
    {
        $this->_builder = $builder;
        $cols = [];
        $vals = [];
        foreach ($this->_builder->put as $k => $v) {
            $cols[] = $k;
            $vals[] = '?';
        }
        $this->_builder->bind($this->_builder->put);
        return 'INSERT INTO '. $this->_builder->table .' ('. implode(', ', $cols) .') VALUES ('. implode(', ', $vals) . ')';
    }

    public function getUpdate(Builder $builder)
    {
        $this->_builder = $builder;
        $sets = [];
        foreach ($this->_builder->put as $k => $v) {
            if ($this->_isRaw($v)) {
                $val = $this->_getRawValue($v);
                unset($this->_builder->put[$k]);
            } else {
                $val = '?';
            }
            $sets[] = $k . '=' . $val;
        }
        $this->_builder->bind($this->_builder->put);
        $where = $this->_builder->wheres ? (' '. $this->_asblWheres()) : '';
        return 'UPDATE '. $this->_builder->table .' SET '. implode(',', $sets) . $where;
    }

    public function getDelete(Builder $builder)
    {
        $this->_builder = $builder;
        $where = $this->_builder->wheres ? (' '. $this->_asblWheres()) : '';
        return 'DELETE FROM '. $this->_builder->table . $where;
    }

    protected function _asblSelect()
    {
        $distinct = $this->_builder->distinct ? ' DISTINCT' : '';
        return 'SELECT'. $distinct .' '. $this->_columnize($this->_builder->select) .' FROM '. $this->_builder->table;
    }

    protected function _asblJoins()
    {
        $join = [];
        foreach ($this->_builder->joins as $item) {
            $join[] = $item['type'] .' JOIN '. $item['table'] .' ON '. $item['on'];
            $this->_builder->bind($item['bind']);
        }
        return implode(' ', $join);
    }

    protected function _asblGroup()
    {
        return 'GROUP BY '. $this->_columnize($this->_builder->group);
    }

    protected function _asblHaving()
    {
        return 'HAVING '. $this->_builder->having;
    }

    protected function _asblOrders()
    {
        $order = [];
        foreach ($this->_builder->orders as $v) {
            if (is_string($v)) {
                $order[] = $v;
            } else {
                $order[] = key($v) .' '. current($v);
            }
        }
        return 'ORDER BY '. implode(',', $order);
    }

    protected function _asblLimit()
    {
        return 'LIMIT '. $this->_builder->limit;
    }

    protected function _asblOffset()
    {
        return 'OFFSET '. $this->_builder->offset;
    }


    protected function _asblWheres()
    {
        $str = '';
        foreach ($this->_builder->wheres as $item) {
            $str .= ' '. $item[0] . ' '. $this->_asblWhereItem($item[1], $item[2]);
        }
        return 'WHERE '. preg_replace('/and |or /i', '', $str, 1);
    }

    /**
     * ('id=? AND name in?', [1, ['aa', 'bb']])
     * ('id=?', 1)
     * ('age>=?', 22)
     * ('id in?', [1,2,3])
     * ('id not in?', [1,2])
     * @param string $condition
     * @param mixed $value
     * @return string
     */
    protected function _asblWhereItem($condition, $value = null)
    {
        if (null === $value) {
            return $condition;
        }
        $sct = substr_count($condition, '?');
        if ($sct == 0) {
            if (is_array($value)) {
                return $condition .' IN('. $this->_asblWhereIn($value) .')';
            } else {
                $this->_builder->bind($value);
                return $condition .'=?';
            }
        }
        $ps = explode('?', $condition);
        if ($sct == 1) {
            $value = [$value];
        }
        // parse: between, like, in and other
        foreach ($ps as $k=>$item) {
            $item = trim($item);
            if (!$item) {
                continue;
            }
            $opt = strtolower(substr($item, -7));
            if ($opt == 'between') {
                $ps[$k] = $item .' '. $this->_asblWhereBetween($value[$k]);
            } elseif (substr($opt, -4) == 'like') {
                $ps[$k] = $item .' '. $this->_asblWhereLike($value[$k]);
            } elseif (substr($opt, -2) == 'in') {
                $ps[$k] = $item .' ('. $this->_asblWhereIn($value[$k]) .')';
            } else {
                $this->_builder->bind($value[$k]);
                $ps[$k] = $item .'?';
            }
        }
        return implode(' ', $ps);
    }

    protected function _asblWhereIn($value)
    {
        if (is_string($value)) {
            $value = explode(',', $value);
        }
        $this->_builder->bind($value);
        return implode(',', array_fill(0, count($value), '?'));
    }

    protected function _asblWhereBetween($value)
    {
        if (!is_array($value)) {
            $value = explode(',', $value);
        } else {
            $value = array_values($value);
        }
        if (!isset($value[1])) {
            throw new \InvalidArgumentException('invalid between condition');
        }
        $this->_builder->bind($value);
        return '? AND ?';
    }

    protected function _asblWhereLike($value)
    {
        if (false === strpos($value, '%')) {
            $value = '%'. $value .'%';
        }
        $this->_builder->bind($value);
        return '?';
    }


    /**
     * Map wrap columns
     * @param $columns
     * @return string
     */
    protected function _columnize($columns)
    {
        if (is_string($columns)) {
            return $columns;
        } else {
            return implode(', ', $columns);
        }
    }

    /**
     * @param Raw $item
     * @return mixed
     */
    protected function _getRawValue($item)
    {
        return $item->getValue();
    }

    /**
     * @param $value
     * @return bool
     */
    protected function _isRaw($value)
    {
        return $value instanceof Raw;
    }
}