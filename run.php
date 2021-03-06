<?php

require __DIR__ . '/vendor/autoload.php';
$dotenv = new Dotenv\Dotenv(__DIR__);
$dotenv->load();

if (!isset($argv[1]) || !isset($argv[2])){
    echo "\n";
    echo "Error: Make sure to pass the legacy provider ID then the continuing provider ID when running this script!";
    echo "\n";
    echo "Required Params (in order): {legacyProviderID(int)} {continuingID(int)} {dryRun(boolean)}";
    echo "\n";
    echo "Example Usage: php run.php 33 122 true";
    echo "\n\n";
    exit;
}

if(!isset($argv[3])){
    echo "\n";
    echo "Error: Make sure to pass dry run flag";
    echo "\n";
    echo "Required Params (in order): {legacyProviderID(int)} {continuingProviderID(int)} {dryRun(boolean)}";
    echo "\n";
    echo "Example Usage: php run.php 123 444 false";
    echo "\n\n";
    exit;
}

$legacyProviderID = $argv[1];
$continuingProviderID = $argv[2];
$dryRun = $argv[3];

use Salestreamsoft\ProviderMerge\ProviderMerge as PM;

$providerMerge = new PM($legacyProviderID, $continuingProviderID, $dryRun);
$providerMerge->start();
