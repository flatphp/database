<?php namespace Flatphp\Database\Sql;

use Flatphp\Database\Connection;

/**
 * @method selectPrepare
 * @method fetchAll
 * @method fetchAllIndexed
 * @method fetchAllGrouped
 * @method fetchRow
 * @method fetchColumn
 * @method fetchPairs
 * @method fetchPairsGrouped
 * @method fetchOne
 * @method execute
 * @method getLastInsertId
 */
class Builder
{
    const TYPE_SELECT = 'select';
    const TYPE_INSERT = 'insert';
    const TYPE_UPDATE = 'update';
    const TYPE_DELETE = 'delete';

    /**
     * @var BaseGrammar
     */
    protected $_grammar;
    /**
     * @var Connection
     */
    protected $_connection;
    protected $_type = self::TYPE_SELECT;
    protected $_bind = [];

    public $table;
    public $distinct = false;
    public $select = '*';
    public $joins;
    public $wheres;
    public $group;
    public $having;
    public $orders;
    public $limit;
    public $offset;

    public $put = [];

    protected $_sql;


    public function __construct(BaseGrammar $grammar, $connection = null)
    {
        $this->_grammar = $grammar;
        if ($connection && !$connection instanceof Connection) {
            throw new \InvalidArgumentException('Parameter 2 for sql builder should be instance of Connection');
        }
        $this->_connection = $connection;
    }

    public function table($table)
    {
        $this->table = trim($table);
        return $this;
    }

    public function distinct()
    {
        $this->_distinct = true;
        return $this;
    }

    public function select($selection, $table = null)
    {
        if ($table) {
            $this->table($table);
        }
        $this->select = $selection;
        return $this;
    }


    /**
     * @param string $type left,right,full,inner
     * @param string|array $table
     * @param string $on 'a.id=b.a_id'
     * @param mixed $bind
     * @return $this
     */
    protected function _join($type, $table, $on, $bind = null)
    {
        $this->joins[] = array(
            'type' => $type,
            'table' => $table,
            'on' => $on,
            'bind' => $bind
        );
        return $this;
    }

    public function leftJoin($table, $on, $bind = null)
    {
        return $this->_join('LEFT', $table, $on, $bind);
    }

    public function rightJoin($table, $on, $bind = null)
    {
        return $this->_join('RIGHT', $table, $on, $bind);
    }

    public function innerJoin($table, $on, $bind = null)
    {
        return $this->_join('INNER', $table, $on, $bind);
    }

    public function fullJoin($table, $on, $bind = null)
    {
        return $this->_join('FULL', $table, $on, $bind);
    }

    public function group($column, $having = null)
    {
        $this->group = $column;
        if ($having) {
            $this->having = $having;
        }
        return $this;
    }

    /**
     * orderBy('id DESC') | orderBy('id, name DESC')
     * orderBy(['id' => 'DESC', 'name' => 'ASC'])
     * @param array|string $column
     * @return $this
     */
    public function orderBy($column)
    {
        $this->orders[] = $column;
        return $this;
    }

    public function limit($number, $offset = null)
    {
        $this->limit = (int)$number;
        if (null !== $offset) {
            $this->offset($offset);
        }
        return $this;
    }

    public function offset($offset)
    {
        $this->offset = (int)$offset;
        return $this;
    }

    /**
     * Set limit and offset by page
     * @param int $page
     * @param int $page_size
     * @return $this
     */
    public function page($page, $page_size = 20)
    {
        $page = (int)$page;
        $page_size = (int)$page_size;
        if ($page < 1) {
            $page = 1;
        }
        $this->limit($page_size);
        if ($page > 1) {
            $this->offset(($page - 1) * $page_size);
        }
        return $this;
    }

    /**
     * e.g.
     * ('id=?', 1)
     * ('id=? AND name=?', [1, 'peter'])
     *
     * (['id' => [1,2,3], 'name' => 'peter'])
     * (['id=1', 'name' => 'peter'])
     * (['id=? AND name=?' => [1, 'peter'], 'gender' => 1])
     *
     * ['and' => ['id' => 1, 'name' => 'peter'], 'or' => ['id=?' => 2, '...']] // this is to complicated, prefer to use raw
     * @return $this
     */
    public function where($where, $bind = null)
    {
        return $this->_where('AND', $where, $bind);
    }

    public function orWhere($where, $bind = null)
    {
        return $this->_where('OR', $where, $bind);
    }

    protected function _where($type, $where, $bind = null)
    {
        if (is_array($where)) {
            foreach ($where as $k=>$v) {
                if (is_int($k)) {
                    $this->wheres[] = [$type, $k, null];
                } else {
                    $this->wheres[] = [$type, $k, $v];
                }
            }
        } else {
            $this->wheres[] = [$type, $where, $bind];
        }
        return $this;
    }


    public function insert($table, $data = null)
    {
        $this->_type = self::TYPE_INSERT;
        if (null === $data && is_array($table)) {
            $this->put = $table;
        } else {
            $this->table($table);
            $this->put = $data;
        }
        return $this;
    }


    public function update($table, $data = null, $where = null)
    {
        $this->_type = self::TYPE_UPDATE;
        if (null === $data && is_array($table)) {
            $this->put = $table;
        } else {
            $this->table($table);
            $this->put = $data;
            if ($where) {
                $this->where($where);
            }
        }
        return $this;
    }

    public function delete($table = null, $where = null)
    {
        $this->_type = self::TYPE_DELETE;
        if ($table) {
            $this->table($table);
        }
        if ($where) {
            $this->where($where);
        }
        return $this;
    }

    public function getType()
    {
        return $this->_type;
    }

    public function getSql()
    {
        if (!$this->_sql) {
            $method = 'get'. ucfirst($this->_type);
            $this->_sql = $this->_grammar->$method($this);
        }
        return $this->_sql;
    }

    /**
     * @return array
     */
    public function getBind()
    {
        return $this->_bind;
    }

    /**
     * Bind value
     * @param mixed $bind
     */
    public function bind($bind)
    {
        if (null === $bind) {
            return;
        }
        if (is_array($bind)) {
            $this->_bind = array_merge($this->_bind, array_values($bind));
        } else {
            $this->_bind[] = $bind;
        }
    }

    public function __call($name, $arguments)
    {
        // try get connection instance
        if (null === $this->_connection) {
            $this->_connection = \Flatphp\Database\DB::getConnection();
        }
        if ($name == 'getLastInsertId') {
            return $this->_connection->getLastInsertId();
        } else {
            return $this->_connection->$name($this->getSql(), $this->getBind());
        }
    }
}