<?php namespace Flatphp\Database;

use PDO;

/**
 * config
 * array(
       'dsn' => '',
       'username' => '',
       'password' => '',
       'options' => [PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8'],
       'slave' => array(
           'dsn' => '',
           'username' => '',
           'password' => ''
       )
)
 */
class Connection
{
    protected $_master_conf = [];
    protected $_slave_conf = [];
    protected $_txns = 0; // transactions
    protected $_options = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION];

    public function __construct(array $config)
    {
        if (!empty($config['slave'])) {
            $this->_slave_conf = $config['slave'];
            unset($config['slave']);
        }
        $this->_master_conf = $config;
        if (!empty($config['options'])) {
            $this->_options = $this->_getOptions($config['options']);
        }
    }

    /**
     * @return PDO
     */
    public function getPDO()
    {
        return $this->getMaster();
    }

    /**
     * @return PDO
     */
    public function getMaster()
    {
        static $pdo = null;
        if (null === $pdo) {
            $pdo = $this->_connect($this->_master_conf, $this->_options);
        }
        return $pdo;
    }

    /**
     * @return PDO
     */
    public function getSlave()
    {
        if (empty($this->_slave_conf) || $this->_txns > 0) {
            return $this->getMaster();
        }
        static $pdo = null;
        if (null === $pdo) {
            $options = empty($this->_slave_conf['options']) ? $this->_options : $this->_getOptions($this->_slave_conf['options']);
            $pdo = $this->_connect($this->_slave_conf, $options);
        }
        return $pdo;
    }

    /**
     * merge options array
     */
    protected function _getOptions($options)
    {
        return array_diff_key($this->_options, $options) + $options;
    }

    /**
     * @param array $config
     * @param array $config
     * @param array $options
     * @return PDO
     */
    protected function _connect($config, $options)
    {
        if (empty($config['dsn'])) {
            throw new \InvalidArgumentException('dsn config is required for database connection');
        }
        $username = isset($config['username']) ? $config['username'] : null;
        $password = isset($config['password']) ? $config['password'] : null;
        return new PDO($config['dsn'], $username, $password, $options);
    }

    /**
     * pdo prepare for select
     * @param string $sql
     * @param mixed $bind
     * @param int $fetch_mode PDO fetch mode
     * @return \PDOStatement
     */
    public function selectPrepare($sql, $bind = null, $fetch_mode = null)
    {
        $stmt = $this->getSlave()->prepare($sql);
        $stmt->execute($this->_bind($bind));
        if (null !== $fetch_mode) {
            $stmt->setFetchMode($fetch_mode);
        }
        return $stmt;
    }

    /**
     * fetch all array with assoc, empty array returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchAll($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * fetch all with firest field as indexed key, empty array returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchAllIndexed($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetchAll(PDO::FETCH_UNIQUE | PDO::FETCH_ASSOC);
    }

    /**
     * fetch all grouped array with first field as keys, empty array returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchAllGrouped($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetchAll(PDO::FETCH_GROUP | PDO::FETCH_ASSOC);
    }

    /**
     * fetch one row array with assoc, empty array returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchRow($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * fetch first column array, empty array returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchColumn($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    /**
     * fetch pairs of first column as Key and second column as Value, empty array returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchPairs($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetchAll(PDO::FETCH_KEY_PAIR);
    }

    /**
     * fetch grouped pairs of K/V with first field as keys of grouped array, empty array returned if nothing of false
     * @param string $sql
     * @param mixed $bind
     * @return array
     */
    public function fetchPairsGrouped($sql, $bind = null)
    {
        $data = [];
        foreach ($this->selectPrepare($sql, $bind)->fetchAll(PDO::FETCH_NUM) as $row) {
            $data[$row[0]] = [$row[1] => $row[2]];
        }
        return $data;
    }

    /**
     * fetch one column value, false returned if nothing or false
     * @param string $sql
     * @param mixed $bind
     * @return mixed
     */
    public function fetchOne($sql, $bind = null)
    {
        return $this->selectPrepare($sql, $bind)->fetchColumn(0);
    }

    /**
     * Execute an SQL statement and return the boolean result.
     * @param string $sql
     * @param mixed $bind
     * @param int &$affected_rows
     * @return bool
     */
    public function execute($sql, $bind = null, &$affected_rows = 0)
    {
        $stmt = $this->getMaster()->prepare($sql);
        $res = $stmt->execute($this->_bind($bind));
        $affected_rows = $stmt->rowCount();
        return $res;
    }

    public function insert($sql, $bind = null, &$insert_id = 0)
    {
        $res = $this->execute($sql, $bind);
        $insert_id = $this->getLastInsertId();
        return $res;
    }

    public function update($sql, $bind = null, &$affected_rows = 0)
    {
        return $this->execute($sql, $bind, $affected_rows);
    }

    public function delete($sql, $bind = null, &$affected_rows = 0)
    {
        return $this->execute($sql, $bind, $affected_rows);
    }

    /**
     * Get last insert id
     * @return int|string
     */
    public function getLastInsertId()
    {
        return $this->getMaster()->lastInsertId();
    }

    /**
     * Execute a Closure within a transaction.
     * @param \Closure $callback
     * @return mixed
     * @throws \Exception
     */
    public function transaction(\Closure $callback)
    {
        $this->beginTransaction();
        try {
            $result = $callback($this);
            $this->commit();
        } catch (\Exception $e) {
            $this->rollBack();
            throw $e;
        }
        return $result;
    }

    /**
     * Start a new database transaction.
     * @return void
     */
    public function beginTransaction()
    {
        ++$this->_txns;
        if ($this->_txns == 1) {
            $this->getMaster()->beginTransaction();
        }
    }

    /**
     * Commit the active database transaction.
     * @return void
     */
    public function commit()
    {
        if ($this->_txns == 1) {
            $this->getMaster()->commit();
        }
        --$this->_txns;
    }

    /**
     * Rollback the active database transaction.
     * @return void
     */
    public function rollBack()
    {
        if ($this->_txns == 1) {
            $this->getMaster()->rollBack();
            $this->_txns = 0;
        } else {
            --$this->_txns;
        }
    }

    /**
     * Parse bind as array
     * @param mixed $bind
     * @return null|array
     */
    protected function _bind($bind)
    {
        if ($bind === null) {
            return null;
        }
        if (!is_array($bind)) {
            $bind = [$bind];
        }
        return $bind;
    }
}