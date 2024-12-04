# Proceso ETL con Tye Express de Akapol

## Introducción

Este documento describe el proceso de Extracción, Transformación y Carga (ETL) utilizando Tye Express para la empresa Akapol. El objetivo de este proceso es integrar datos de diferentes fuentes y cargarlos en un sistema de gestión de datos para su análisis y uso posterior.

## Requisitos Previos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes:

- Python 3.x
- Acceso a la base de datos de SQL Server
- Archivo `.env` configurado con las siguientes variables:
  - `PATH_LOG`: Ruta donde se guardarán los logs.
  - `LOG_NAME`: Nombre del archivo de log.
  - `BASE_AKAPOL`: Nombre de la base de datos de Akapol.
  - `SERVER`: Dirección del servidor de la base de datos.
  - `USER`: Nombre de usuario para la base de datos.
  - `PASSWORD`: Contraseña para la base de datos.
  - `API_KEY`: Clave de API para acceder a servicios externos.
  - `URL`: URL del servicio web de Tye.
  - `PATH_PDF`: Ruta donde se guardarán los archivos PDF generados.
  - `PATH_APP`: Ruta donde se encuentra la aplicación ejecutable.

## Proceso ETL

### 1. Extracción

La extracción de datos se realiza desde la base de datos de Akapol y otros servicios externos. Se utilizan consultas SQL para obtener la información necesaria. El proceso de extracción se lleva a cabo en la clase `Connection`, que maneja la conexión a la base de datos y la ejecución de consultas.

### 2. Transformación

Una vez que los datos son extraídos, se transforman para cumplir con los requisitos del sistema de destino. Esto incluye la limpieza de datos, la conversión de formatos y la validación de la información. Las clases `CashAdvance`, `Expense`, `Report`, y `Notifier` son responsables de estructurar y validar los datos.

### 3. Carga

Los datos transformados se cargan en el sistema de destino. Esto se realiza mediante la ejecución de procedimientos almacenados en la base de datos de Akapol. La clase `Inserter` se encarga de insertar los datos de avances de efectivo y reportes en la base de datos.

### 4. Notificación

Después de la carga, se envían notificaciones sobre el estado del proceso ETL. Esto se realiza a través de la clase `Notifier`, que genera mensajes de notificación basados en el resultado de las inserciones.

## Ejecución del Proceso ETL

Para ejecutar el proceso ETL, sigue estos pasos:

1. Asegúrate de que el archivo `.env` esté correctamente configurado.
2. Ejecuta el script principal: "python src/main.py"
3. Revisa los logs generados en la ruta especificada en `PATH_LOG` para verificar el estado del proceso.

## Manejo de Errores

En caso de que ocurra un error durante el proceso ETL, se registrará en el archivo de log y se enviará un correo electrónico de notificación. Asegúrate de que la configuración de correo electrónico en la base de datos esté correctamente configurada para recibir estas notificaciones.
