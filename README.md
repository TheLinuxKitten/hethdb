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
