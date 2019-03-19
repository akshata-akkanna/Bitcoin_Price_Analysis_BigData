create database blockchains;

use blockchains;

create external table BitcoinBlockchain ROW FORMAT SERDE 'org.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/bitcoin/input' TBLPROPERTIES("hadoopcryptoledger.bitcoinblockinputformat.filter.magic"="F9BEB4D9");

create external table BitcoinBlockchainTestNet3 ROW FORMAT SERDE 'org.hadoop.bitcoin.hive.serde.BitcoinBlockSerde' STORED AS INPUTFORMAT 'org.hadoop.bitcoin.format.mapred.BitcoinBlockFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.mapred.lib.NullOutputFormat' LOCATION '/user/bitcoin/input' TBLPROPERTIES("hadoopcryptoledger.bitcoinblockinputformat.filter.magic"="0B110907");


//Count Example
select count(*) from BitcoinBlockchain;

select count(*) from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions;

select SUM(expout.value) FROM (select * from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode(exptran.listofoutputs) exploded_outputs as expout;

select count(*) FROM (select * from BitcoinBlockchain LATERAL VIEW explode(transactions) exploded_transactions as exptran) transaction_table LATERAL VIEW explode(exptran.listofoutputs) exploded_outputs as expout WHERE instr(substring(regexp_extract(hex(expout.txoutscript),"76A914.*88AC",0),7,41),"C825A1ECF2A6830C4401620C3A16F1995057C2AB")>0;

