<?php

namespace Salestreamsoft\ProviderMerge;

use Salestreamsoft\ProviderMerge\DbConnection as DB;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

/**
 * Class ProviderMerge
 * @package Salestreamsoft\ProviderMerge
 */
class ProviderMerge
{
    private $legacyProviderId;
    private $continuingProviderId;
    private $dryRun;

    private $providerIds;
    private $db;
    private $log;

    private $table;
    private $possibleAffectedTables;

    private $preMergeTableRecordCounts;
    private $postMergeTableRecordCounts;

    private $commSchemasCurrentlyReset;

    private $pgDumpCommand = '';
    private $truncateTablesCommand;
    private $restoreDbCommand;
    private $pgDumpTables;

    /**
     * ProviderMerge constructor.
     * @param $legacyProviderId
     * @param $continuingProviderId
     * @param $dryRun
     */
    public function __construct($legacyProviderId, $continuingProviderId, $dryRun)
    {
        $this->legacyProviderId = $legacyProviderId;
        $this->continuingProviderId = $continuingProviderId;
        $this->dryRun = $dryRun;
        $this->providerIds = array($this->legacyProviderId, $this->continuingProviderId);
        $this->db = new DB();
        $this->commSchemasCurrentlyReset = array();

        $this->log = new Logger('log');
        $this->log->pushHandler(new StreamHandler(getenv('LOG_PATH')));
    }

    /**
     * Start command - which will run all necessary methods
     * to complete provider merge
     */
    public function start(){

        $this->db->beginTransaction();

        $this->setPossibleAffectedTables();
        $this->mergeProviders();
        $this->resetLegacyOrderRelationships();
        $this->deactivateLegacyProvider();

        if($this->dryRun == "true"){
            if($this->pgDumpTables){
                $this->buildPgDumpCommand();
                $this->buildTruncateCommand();
                $this->buildRestoreDbCommand();
                $this->finalizeDryRun();
            }else{
                echo "\n";
                echo "No tables were affected by this provider merge";
                echo "\n\n";
                die();
            }
        }else{
            echo "\n\n";
            echo 'Finished provider merge with no errors';
            echo "\n";
            $this->db->commit();
            die();
        }
    }

    /**
     *  Query DB to determine which tables have a provider_id field
     *  to determine which tables will possible be affected
     *  Set Possible Affected Tables array
     */
    private function setPossibleAffectedTables(){
        $sql = "SELECT table_schema, table_name, column_name, data_type
		FROM information_schema.columns
		WHERE is_updatable = 'YES'
			AND (column_name LIKE '%provider_id%' OR column_name LIKE '%api_id%' OR column_name = 'use_iq_code_of')
			AND (table_name NOT LIKE 'a\_%' 
			    AND table_name NOT LIKE 'arc\_%'
				AND table_name NOT LIKE 'invoicing%' 
				AND table_name NOT LIKE 'vw\_%' 
				AND table_name NOT LIKE '%quote_sq_skus%'
				AND table_name NOT LIKE '%quote_providers%'
				AND table_name NOT LIKE '%order_relationships%'
				AND table_name NOT LIKE '%comm_templates%'
				AND table_name NOT IN ('sku_where_statements', 'sq_iq_cache', 'sq_test_quote_xlog'))
		ORDER BY table_name";

        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $tables = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $this->possibleAffectedTables = $tables;
    }

    /**
     * 1st: Loops through all possible affected tables
     * 2nd: Determine if table will be affected by querying for legacy provider records
     * 3rd: If COMM schema, reset any conflicting import template names
     * 4th: Run actual merge routine against table
     * 5th: Validate pre vs. post record counts
     * @return bool
     */
    private function mergeProviders(){
        $this->preMergeTableRecordCounts = array();
        $this->postMergeTableRecordCounts = array();

        foreach ($this->possibleAffectedTables as $key => $table) {
            $this->table = $table;

            $this->preMergeTableRecordCounts = $this->getTableRecordCountsByProviderIds();

            if($this->preMergeTableRecordCounts['legacy_provider_count'] > 0) {

                if (strpos($this->table['table_schema'], 'comm_') !== false) {
                    if(!in_array($this->table['table_schema'], $this->commSchemasCurrentlyReset))
                        $this->resetConflictingCommImportTemplateNames();
                }

                $this->log->addInfo('Starting Provider Merge for Table: ' .$this->table['table_name']);
                $this->log->addInfo('Pre Merge Table Counts', $this->preMergeTableRecordCounts);

                foreach ($this->preMergeTableRecordCounts['legacy_provider_records'] as $k => $legacy_provider_record) {

                    if(!empty($legacy_provider_record)){
                        $this->mergeLegacyProviderRecordsWithContinuingProvider($legacy_provider_record);
                    }
                }

                $this->postMergeTableRecordCounts = $this->getTableRecordCountsByProviderIds();
                $this->log->addInfo('Post Merge Table Counts', $this->postMergeTableRecordCounts);
                $this->validatePreVsPostTableRecordCounts();

                if($this->dryRun == 'true')
                    $this->pgDumpTables[] = $this->table['table_name'];
            }
        }

        return true;
    }

    /**
     * 1st: Checks for unique table constraint
     * 2nd: If Constraint exists - delete legacy provider record
     * 3rd: If no constraint exists - update legacy record with continuing provider_id
     * @param $legacy_provider_record
     * @return bool
     * @throws \Exception
     */
    private function mergeLegacyProviderRecordsWithContinuingProvider($legacy_provider_record){

        $mergeConstraintExists = $this->checkUniqueConstraintExistsInTableByLegacyProviderRecord($legacy_provider_record);

        $whereStatement = '';
        foreach ($legacy_provider_record as $columnName => $columnValue) {
            if ($columnValue != '') {
                $columnValue = "'" . pg_escape_string($columnValue) . "'";
                if (empty($whereStatement) ? $whereStatement = ' WHERE "' . $columnName . '" = ' . $columnValue : $whereStatement .= ' AND "' . $columnName . '" = ' . $columnValue) ;
            }
        }

        if ($mergeConstraintExists) {
            $deleteMergeConstraint = $this->deleteLegacyMergeConstraintRecord($whereStatement);

            if ($deleteMergeConstraint == true)
                return true;

            $this->db->rollBack();
            print_r($deleteMergeConstraint);
            throw new \Exception("Merge Provider Error: Failed to delete legacy records from table with merge constraint");
        }

        $this->updateLegacyProviderRecord($whereStatement);

        return true;
    }

    /**
     * Update legacy provider record's provider_id with continuing
     * provider's provider_id
     * @param $whereStatement
     * @return bool
     * @throws \Exception
     */
    private function updateLegacyProviderRecord($whereStatement){
        try{
            $sqlUpdate = "UPDATE {$this->table['table_schema']}.{$this->table['table_name']} 
                          SET {$this->table['column_name']} = '{$this->continuingProviderId}'";

            $sqlUpdate .= $whereStatement;

            $stmt = $this->db->prepare($sqlUpdate);
            $stmt->execute();

            $affectedRows = $stmt->rowCount();

            if( $affectedRows == 0){
                $response['message'] = "Error: Failed to update legacy provider record to continuing provider id";
                $response['sql'] = $sqlUpdate;
                $response['sql_exception'] = '';
                print_r($response);
                $this->db->rollBack();
                throw new \Exception("Merge Provider Error: Failed to update legacy provider record to continuing provider id");
            }

            $this->log->addInfo('Completed SQL');
            $this->log->addInfo($sqlUpdate);

            return true;
        }catch(\PDOException $e){

            $response['message'] = "Error: SQL Query Error";
            $response['sql'] = $sqlUpdate;
            $response['sql_exception'] = $e->getMessage();
            print_r($response);
            $this->db->rollBack();
            throw $e;
        }
    }

    /**
     * Delete all legacy records that conflict with a unique constraint
     * on continuing provider records for a specific table
     * @param $whereStatment
     * @return bool
     * @throws \Exception
     */
    private function deleteLegacyMergeConstraintRecord($whereStatment){
        try{
            $deleteLegacyRecordConstraintSql = "DELETE FROM {$this->table['table_schema']}.{$this->table['table_name']}";
            $deleteLegacyRecordConstraintSql .= $whereStatment;
            $stmt = $this->db->prepare($deleteLegacyRecordConstraintSql);
            $stmt->execute();

            $affectedRows = $stmt->rowCount();
            if( $affectedRows > 0){
                $this->log->addInfo('Completed SQL');
                $this->log->addInfo($deleteLegacyRecordConstraintSql);
                $this->preMergeTableRecordCounts['legacy_provider_count'] = $this->preMergeTableRecordCounts['legacy_provider_count'] - 1;
                $this->preMergeTableRecordCounts['combined_provider_count'] = $this->preMergeTableRecordCounts['combined_provider_count'] - 1;
                return true;
            }

            $response['message'] = "Error: Delete Query did not affect any rows";
            $response['sql'] = $deleteLegacyRecordConstraintSql;
            $response['sql_exception'] = '';
            print_r($response);
            $this->db->rollBack();
            throw new \Exception("Merge Provider Error");

        }catch(\PDOException $e){

            $response['message'] = "Error: SQL Query Error";
            $response['sql'] = $deleteLegacyRecordConstraintSql;
            $response['sql_exception'] = $e->getMessage();
            print_r($response);
            $this->db->rollBack();
            throw $e;
        }

    }

    /**
     * Check to see if specified table has a unique constraint
     * @param $legacy_provider_record
     * @return array
     */
    private function checkUniqueConstraintExistsInTableByLegacyProviderRecord($legacy_provider_record)
    {

        $tableConstraintColumnsSql = "SELECT kcu.column_name, constraint_type
                                             FROM information_schema.table_constraints AS tc 
                                             JOIN information_schema.key_column_usage AS kcu
                                              ON tc.constraint_name = kcu.constraint_name
                                             WHERE tc.table_schema = '".$this->table['table_schema']."' 
                                             AND tc.table_name = '" . $this->table['table_name'] . "'";

        $stmt = $this->db->prepare($tableConstraintColumnsSql);
        $stmt->execute();
        $tableConstraintColumns = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $tableConstraintColumns = array_map("unserialize", array_unique(array_map("serialize", $tableConstraintColumns)));

        foreach ($legacy_provider_record as $columnName => $columnValue) {
            $whereStatement = " WHERE {$this->table['column_name']} = '{$this->continuingProviderId}'";
            foreach ($tableConstraintColumns as $key => $constraintColumn) {
                if ($columnName == $constraintColumn['column_name'] && $columnName != $this->table['column_name']) {
                    $mergeConstraintExistsSql = "SELECT * FROM {$this->table['table_schema']}.{$this->table['table_name']}";
                    $mergeConstraintExistsSql .= $whereStatement." AND {$columnName} = '{$columnValue}'";

                    $stmt = $this->db->prepare($mergeConstraintExistsSql);
                    $stmt->execute();
                    $constraintExists = $stmt->fetchAll(\PDO::FETCH_ASSOC);

                    if($constraintExists)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * Validates pre vs. post table record counts for both
     * legacy and continuing provider ids
     * @throws \Exception
     */
    private function validatePreVsPostTableRecordCounts(){
        if($this->postMergeTableRecordCounts['legacy_provider_count'] > 0){
            $this->db->rollBack();
            print_r($this->preMergeTableRecordCounts);
            print_r($this->postMergeTableRecordCounts);
            throw new \Exception("Merge Provider Error: Legacy Provider records still exist in '{$this->table['table_name']}''");
        }

        if($this->preMergeTableRecordCounts['combined_provider_count'] != $this->postMergeTableRecordCounts['combined_provider_count']){

            print_r($this->preMergeTableRecordCounts);
            print_r($this->postMergeTableRecordCounts);
            $this->db->rollBack();
            throw new \Exception("Merge Provider Error: Pre-Merge combined provider record counts, do not match post-merge combined provider record counts for '{$this->table['table_name']}''");
        }
    }

    /**
     * Gets all record counts for both the legacy provider
     * and the continuing provider for a specific table
     * @return array
     */
    private function getTableRecordCountsByProviderIds(){
        $tableRecordCounts = array();
        $tableRecordCounts['combined_provider_count'] = 0;
        $tableRecordCounts['legacy_provider_count'] = 0;

        foreach($this->providerIds as $key => $providerId) {
            $tableName = $this->table['table_name'];
            $columnName = $this->table['column_name'];
            $tableSchema = $this->table['table_schema'];

            $sql = "SELECT *
                    FROM $tableSchema.$tableName
                    WHERE $columnName = '$providerId'";

            $stmt = $this->db->prepare($sql);
            $stmt->execute();
            $results = $stmt->fetchAll(\PDO::FETCH_ASSOC);


            if ($results){
                $tableRecordCounts['combined_provider_count'] = $tableRecordCounts['combined_provider_count'] + count($results);

                if((int)$providerId === (int)$this->legacyProviderId){
                    $tableRecordCounts['legacy_provider_count'] = count($results);
                    $tableRecordCounts['legacy_provider_records'] = $results;
                }
            }
        }

        return $tableRecordCounts;
    }

    /**
     * Reset's all legacy order_relationships
     * Example 'CONTINUING_PROVIDER_NAME (for LEGACY_PROVIDER_NAME)'
     * @throws \Exception
     */
    private function resetLegacyOrderRelationships(){

        $sql = "
                SELECT provider 
                       || ' (for ' 
                       || (SELECT provider FROM quote_providers WHERE id = $this->legacyProviderId) 
                       || ')' as name 
                FROM quote_providers 
                WHERE id = $this->continuingProviderId;
        ";

        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $newMasterAgentName = $stmt->fetch(\PDO::FETCH_ASSOC);

        $sql = "        
                UPDATE order_relationships
                SET master_agent = '{$newMasterAgentName['name']}'
                WHERE provider_id IN ($this->legacyProviderId);
            ";

        try{
            $stmt = $this->db->prepare($sql);
            $stmt->execute();
            $affectedRows = $stmt->rowCount();

            if($affectedRows < 1){
                $response['message'] = "Error: SQL Query Error";
                $response['sql'] = $sql;
                $response['sql_exception'] = '';
                print_r($response);

                $this->db->rollBack();
                throw new \Exception("Merge Provider Error: Failed to reset order relationships");
            }
        }catch(\PDOException $e){

            $response['message'] = "Error: SQL Query Error";
            $response['sql'] = $sql;
            $response['sql_exception'] = $e->getMessage();
            print_r($response);
            $this->db->rollBack();
            throw $e;
        }
    }

    /**
     * Set's legacy provider to inactive = true
     * in quote_providers table
     * @throws \Exception
     */
    private function deactivateLegacyProvider(){
        $sql = "        
          UPDATE quote_providers SET inactive = TRUE WHERE id IN ($this->legacyProviderId);
        ";

        try{
            $stmt = $this->db->prepare($sql);
            $stmt->execute();
            $affectedRows = $stmt->rowCount();

            if($affectedRows < 1){
                $response['message'] = "Error: SQL Query Error";
                $response['sql'] = $sql;
                $response['sql_exception'] = '';
                print_r($response);

                $this->db->rollBack();
                throw new \Exception("Merge Provider Error: Failed to deactivate legacy provider");
            }
        }catch(\PDOException $e){

            $response['message'] = "Error: SQL Query Error";
            $response['sql'] = $sql;
            $response['sql_exception'] = $e->getMessage();
            print_r($response);
            $this->db->rollBack();
            throw $e;
        }
    }

    /**
     * Reset's all legacy template names
     * Example: 'TEMPLATE_NAME - LEGACY_PROVIDER_NAME'
     * @throws \Exception
     */
    public function resetConflictingCommImportTemplateNames(){
        $sql = "
            UPDATE {$this->table['table_schema']}.comm_templates AS t1
            SET template_name = template_name || ' - ' || (SELECT provider FROM quote_providers WHERE id = t1.provider_id)
            WHERE provider_id IN ($this->legacyProviderId);
        ";

        try{
            $stmt = $this->db->prepare($sql);
            $stmt->execute();

            array_push($this->commSchemasCurrentlyReset, $this->table['table_schema']);

        }catch(\PDOException $e){

            $response['message'] = "Error: SQL Query Error";
            $response['sql'] = $sql;
            $response['sql_exception'] = $e->getMessage();
            print_r($response);
            $this->db->rollBack();
            throw $e;
        }
    }

    /**
     * Echo's out appropriate Dry Run backup commands
     * and performs a rollback
     */
    private function finalizeDryRun(){
        echo "\n";
        echo 'Finished provider merge dry-run with rollback and no errors';
        echo "\n\n";
        echo "pg_dump backup command: ";
        echo "\n\n";
        echo $this->pgDumpCommand;
        echo "\n\n";
        echo "Trucate Affected Tables Command: ";
        echo "\n\n";
        echo $this->truncateTablesCommand;
        echo "\n\n";
        echo "pg_restore command: ";
        echo "\n\n";
        echo $this->restoreDbCommand;
        echo "\n\n";
        $this->db->rollBack();
        die();
    }

    /**
     * build DB PG Dump Command String with affected tables
     */
    private function buildPgDumpCommand(){
        if(!$this->pgDumpCommand)
            $this->pgDumpCommand = 'pg_dump -p8100 -hlocalhost -Udevteam -Fc --data-only';

        foreach($this->pgDumpTables as $key => $table){
            $this->pgDumpCommand .= ' -t '.$table;
        }

        $this->pgDumpCommand .= ' DEV_MAM > /tmp/provider_merge_backup.dump';
    }

    /**
     * builds DB Truncate Tables Command String with affected tables
     */
    private function buildTruncateCommand(){

        $this->truncateTablesCommand = "psql -p8100 -hlocalhost -Udevteam -dDEV_MAM -c 'TRUNCATE ";

        foreach($this->pgDumpTables as $key => $table){
            if($key == 0 ? $this->truncateTablesCommand .= $table : $this->truncateTablesCommand .= ', '. $table);
        }

        $this->truncateTablesCommand .= "'";
    }

    /**
     * builds DB Restore Command String
     */
    private function buildRestoreDbCommand(){
        $this->restoreDbCommand = "pg_restore -p8100 -hlocalhost -Udevteam -dDEV_MAM /tmp/provider_merge_backup.dump";
    }

}