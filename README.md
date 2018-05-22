# hethdb

Utilidad y librería para crear y mantener una base de datos *MySQL* (`ethdb`) con información asociada a los accounts presentes en el blockchain *Ethereum*.

La inicialización de la base de datos crea las tablas y sus indices en la base de datos. Inserta las direcciones de los accounts del bloque génesis.

Su funcionamiento normal es procesar los bloques uno a uno. Obtiene la información asociada al bloque y la inserta con una transacción *MySQL*.

En cualquier momento se puede detener la aplicación pulsando `Ctrl+C`.

## Opciones del programa

```
  [--cmd <comando>]                 Acción a realizar (Actualizar la base de datos)
  [--myHttp <dirección>]            Dirección de la instancia MySQL (localhost)
  [--myPort <puerto>]               Puerto de la instancia MySQL (3306)
  [--ethHttp <url>]                 URL del nodo geth (http://localhost:8545)
  [--initDb]                        Inicializa (crea) la base de datos MySQL (OFF)
  [--iniBlk <num>]                  Bloque de inicio del proceso (último bloque más 1)
  [--numBlks <num>]                 Nro de bloques a procesar (100)
  [--par]                           Analiza los traces, de las transacciones de
                                    un bloque, en paralelo (OFF)
  [--log]                           Activa los logs de la librería hethrpc (OFF)
  [--test]                          No inserta información en la base de datos (OFF)
  [-h|--help]                       Muestra las opciones de uso
```

Si no se especifica comando la acción por defecto es actualizar la BD. Los comandos válidos son:

```
  disasm                            Desensambla el código leido de stdin
  updateCode                        Solo actualiza los contractsCode
```

## Administrar la base de datos

El script `demo/docker-mariadb` se puede usar para ejecutar un *container docker* y administrar la base de datos.

La opción `--init` establece la contraseña del usuario *root* (`ethereum`), crea el usuario `kitten` con su contraseña (`kitten`), crea la base de datos `ethdb`, y procesa todos los ficheros `.sql` del directorio `mysql` del proyecto.

Las consolas cliente montan el directorio `mysql` del proyecto en la ruta `/user-data.d` del container.

### Opciones del script

```
  [--init]                          Crea la base de datos
  [--run]                           Ejecuta el demonio MySQL
  [--cli-root]                      Ejecuta una consola MySQL con el usuario root
  [--cli-user]                      Ejecuta una consola MySQL con el usuario
                                    propietario de la base de datos
  [--ip]                            Muestra la dirección IP del container que
                                    ejecuta el demonio MySQL
```

En el directorio `mysql` hay procedures:
```
  dropFromBlock                     Elimina entradas de las tablas
  dropTables                        Elimina las tablas
```

## Tokens Ethereum

No existe (de momento) un método práctico para identificar y validar los contracts que implementan tokens.

El método *ideal* consiste en obtener la dirección *bzzr* del código binario del contract, descargar la interfaz ABI de *swarm* y validarlo analizando las funciones y eventos de la interfaz. Pero, actualmente, la mayoría de esta información no está disponible en *swarm*.

El método empleado en esta librería consiste en asumir que todo contract que implementa un token (sea ERC20 o no) emite alguno de los events especificados en ERC20, o sea, `Transfer(address,address,uint256)` y/o `Approval(address,adddress,uint256)`. A partir de esta premisa:

    1. Se obtienen los logs emitidos por estos eventos

    2. Las direcciones que emiten estos eventos son contracts que implementan
       un token. Estas implementaciones pueden o no cumplir la especificación
       ERC20/EIP20.
    
    3. Se obtiene el resto de información de cada token

## La base de datos

```
MariaDB [(none)]> use ethdb;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
MariaDB [ethdb]> show tables;
+-------------------+
| Tables_in_ethdb   |
+-------------------+
| blocks            |
| contractCreations |
| contractsCode     |
| deadAccounts      |
| erc20Logs         |
| erc20s            |
| genesis           |
| internalTxs       |
| msgCalls          |
| txs               |
+-------------------+
10 rows in set (0.001 sec)

MariaDB [ethdb]> show create table genesis;
+---------+----------------------------------------------------------------+
| Table   | Create Table                                                   |
+---------+----------------------------------------------------------------+
| genesis | CREATE TABLE `genesis` (
  `addr` binary(20) NOT NULL,
  `balance` binary(32) NOT NULL,
  PRIMARY KEY (`addr`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1                                     |
+---------+----------------------------------------------------------------+
1 row in set (0.004 sec)

MariaDB [ethdb]> show create table blocks;
+--------+---------------------------------------------------------------+
| Table  | Create Table                                                  |
+--------+---------------------------------------------------------------+
| blocks | CREATE TABLE `blocks` (
  `blkNum` int(10) unsigned NOT NULL,
  `blkHash` binary(32) NOT NULL,
  `miner` binary(20) NOT NULL,
  `difficulty` binary(32) NOT NULL,
  `gasLimit` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`blkNum`),
  KEY `miner` (`miner`),
  KEY `blkHash` (`blkHash`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1                                   |
+--------+---------------------------------------------------------------+
1 row in set (0.001 sec)

MariaDB [ethdb]> show create table txs;
+-------+-----------------------------------------------------------------------+
| Table | Create Table                                                          |
+-------+-----------------------------------------------------------------------+
| txs   | CREATE TABLE `txs` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `txHash` binary(32) NOT NULL,
  `txValue` binary(32) NOT NULL,
  `gas` int(10) unsigned NOT NULL,
  `failed` tinyint(1) NOT NULL,
  `maskOpcodes` bit(64) NOT NULL,
  PRIMARY KEY (`blkNum`,`txIdx`),
  KEY `txHash` (`txHash`),
  CONSTRAINT `txs_ibfk_1` FOREIGN KEY (`blkNum`) REFERENCES `blocks` (`blkNum`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+-------+-----------------------------------------------------------------------+
1 row in set (0.002 sec)

MariaDB [ethdb]> show create table contractCreations;
+-------------------+-----------------------------------------------------------+
| Table             | Create Table                                              |
+-------------------+-----------------------------------------------------------+
| contractCreations | CREATE TABLE `contractCreations` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `fromA` binary(20) NOT NULL,
  `contractA` binary(20) NOT NULL,
  PRIMARY KEY (`blkNum`,`txIdx`),
  KEY `contractCreationFrom` (`fromA`),
  KEY `contractCreationContract` (`contractA`),
  CONSTRAINT `contractCreations_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+-------------------+-----------------------------------------------------------+
1 row in set (0.000 sec)

MariaDB [ethdb]> show create table msgCalls;         
+----------+--------------------------------------------------------------------+
| Table    | Create Table                                                       |
+----------+--------------------------------------------------------------------+
| msgCalls | CREATE TABLE `msgCalls` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `fromA` binary(20) NOT NULL,
  `toA` binary(20) NOT NULL,
  PRIMARY KEY (`blkNum`,`txIdx`),
  KEY `msgCallFrom` (`fromA`),
  KEY `msgCallTo` (`toA`),
  CONSTRAINT `msgCalls_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+----------+--------------------------------------------------------------------+
1 row in set (0.000 sec)

MariaDB [ethdb]> show create table internalTxs;
+-------------+-----------------------------------------------------------------+
| Table       | Create Table                                                    |
+-------------+-----------------------------------------------------------------+
| internalTxs | CREATE TABLE `internalTxs` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `idx` mediumint(8) unsigned NOT NULL,
  `fromA` binary(20) NOT NULL,
  `addr` binary(20) NOT NULL,
  `opcode` tinyint(3) unsigned NOT NULL,
  PRIMARY KEY (`blkNum`,`txIdx`,`idx`),
  KEY `internalTxFrom` (`fromA`),
  KEY `internalTxAddr` (`addr`),
  CONSTRAINT `internalTxs_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+-------------+-----------------------------------------------------------------+
1 row in set (0.000 sec)

MariaDB [ethdb]> show create table deadAccounts;
+--------------+----------------------------------------------------------------+
| Table        | Create Table                                                   |
+--------------+----------------------------------------------------------------+
| deadAccounts | CREATE TABLE `deadAccounts` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `addr` binary(20) NOT NULL,
  PRIMARY KEY (`addr`),
  KEY `blkNum` (`blkNum`,`txIdx`),
  CONSTRAINT `deadAccounts_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+--------------+----------------------------------------------------------------+
1 row in set (0.000 sec)

MariaDB [ethdb]> show create table contractsCode;
+---------------+---------------------------------------------------------------+
| Table         | Create Table                                                  |
+---------------+---------------------------------------------------------------+
| contractsCode | CREATE TABLE `contractsCode` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `addr` binary(20) NOT NULL,
  `bzzr0` binary(32) DEFAULT NULL,
  `code` mediumblob NOT NULL,
  PRIMARY KEY (`blkNum`,`addr`),
  KEY `contractCodeBzzr0` (`bzzr0`)
  CONSTRAINT `contractsCode_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`),
) ENGINE=InnoDB DEFAULT CHARSET=latin1 |
+---------------+---------------------------------------------------------------+
1 row in set (0.001 sec)

MariaDB [ethdb]> show create table erc20s;       
+--------+----------------------------------------------------------------------+
| Table  | Create Table                                                         |
+--------+----------------------------------------------------------------------+
| erc20s | CREATE TABLE `erc20s` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `addr` binary(20) NOT NULL,
  `isErc20` tinyint(1) NOT NULL,
  `name` binary(255) DEFAULT NULL,
  `symbol` binary(32) DEFAULT NULL,
  `decimals` tinyint(3) unsigned DEFAULT NULL,
  PRIMARY KEY (`addr`),
  KEY `blkNum` (`blkNum`,`txIdx`),
  KEY `blkNum_2` (`blkNum`,`addr`),
  CONSTRAINT `erc20s_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`),
  CONSTRAINT `erc20s_ibfk_2` FOREIGN KEY (`blkNum`, `addr`) REFERENCES `contractsCode` (`blkNum`, `addr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci |
+--------+----------------------------------------------------------------------+
1 row in set (0.001 sec)

MariaDB [ethdb]> show create table erc20Logs;
+-----------+-------------------------------------------------------------------+
| Table     | Create Table                                                      |
+-----------+-------------------------------------------------------------------+
| erc20Logs | CREATE TABLE `erc20Logs` (
  `blkNum` int(10) unsigned NOT NULL,
  `txIdx` smallint(5) unsigned NOT NULL,
  `addr` binary(20) NOT NULL,
  `transfer` tinyint(1) NOT NULL,
  `fromA` binary(20) NOT NULL,
  `toA` binary(20) NOT NULL,
  `amount` binary(32) NOT NULL,
  KEY `blkNum` (`blkNum`,`txIdx`),
  KEY `erc20LogsAddr` (`addr`),
  KEY `erc20LogsFrom` (`fromA`),
  KEY `erc20LogsTo` (`toA`),
  KEY `erc20LogsAmount` (`amount`),
  CONSTRAINT `erc20Logs_ibfk_1` FOREIGN KEY (`blkNum`, `txIdx`) REFERENCES `txs` (`blkNum`, `txIdx`),
  CONSTRAINT `erc20Logs_ibfk_2` FOREIGN KEY (`addr`) REFERENCES `erc20s` (`addr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci |
+-----------+-------------------------------------------------------------------+
1 row in set (0.000 sec)
```

## Comandos útiles

Mostrar los events de contracts ERC20 de 200 bloques desde el 2306366:
```
for i in $(echo "obase=16; j=2306366; for(k=0;k<200;k++) print j+k,\"\n\";" | bc) ; do curl -s -X POST -H 'Content-Type: application/json' --data "{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"params\":[{\"fromBlock\":\"0x$i\",\"toBlock\":\"0x$i\",\"topics\":[[\"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef\",\"0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925\"]]}],\"id\":\"id-1\"}" http://192.168.2.101:8545 | jq '.result' ; done | less
```

Calcular topic0 de los eventos ERC20:
```
Network.Web3.Types Network.Web3.Dapp.EthABI> :set -XOverloadedStrings
Network.Web3.Types Network.Web3.Dapp.EthABI> stripHex $ toHex $ keccak256 "Transfer(address,address,uint256)"
"ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
Network.Web3.Types Network.Web3.Dapp.EthABI> stripHex $ toHex $ keccak256 "Approval(address,address,uint256)"
"8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
```
