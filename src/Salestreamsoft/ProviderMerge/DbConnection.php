<?php

namespace Salestreamsoft\ProviderMerge;

class DbConnection extends \PDO
{
    protected $transactionCounter = 0;

    public function __construct()
    {
        try{

            $connection = 'pgsql:host='.getenv('DB_HOST').';port='.getenv('DB_PORT').';dbname='.getenv('DB_NAME');
            $options = [];

            parent::__construct($connection, getenv('DB_USER'), getenv('DB_PASS'), $options);

            $this->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );

            $this->exec("SET statement_timeout = 0");

        }catch(PDOException  $e ){
            throw new \Exception("PostgreSQL Connection Error: ".$e->getMessage());
        }
    }

    function beginTransaction()
    {
        if(!$this->transactionCounter++)
            return parent::beginTransaction();
        return $this->transactionCounter >= 0;
    }

    function commit()
    {
        if(!--$this->transactionCounter)
            return parent::commit();
        return $this->transactionCounter >= 0;
    }

    function rollback()
    {
        if($this->transactionCounter >= 0)
        {
            $this->transactionCounter = 0;
            return parent::rollback();
        }
        $this->transactionCounter = 0;
        return false;
    }
}