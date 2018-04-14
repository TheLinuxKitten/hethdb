# hethdb

Utilidad y librería para crear y mantener una base de datos MySQL (`ethdb`) con información asociada a los accounts presentes en el blockchain.

La inicialización de la base de datos crea las tablas y sus indices en la base de datos. Obtiene las direcciones de los accounts del bloque génesis.

Procesa los bloques uno a uno. Obtiene la información asociada al bloque y la inserta con una transacción MySQL.

En cualquier momento se puede detener la aplicación pulsando `Ctrl+C`.

## Opciones del programa

```
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

