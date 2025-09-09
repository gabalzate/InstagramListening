Nota.  Los scripts trabajan con dos archivos externos que no están presentes en el repositorio pero deben ser creados para su funcionamiento.

+ config.py
  + Este archivo contiene la clave de la aplicación scrapecreators, ya que todos los scripts están realizados para consultar los endpoints de este.  El archivo solo contiene una línea y es la siguiente:
  + SCRAPE_API_KEY = "xxxxx......zzzzzz"
  + Este API KEY se obtiene en el portal https://app.scrapecreators.com
+ perfiles_instagram.txt
  + Este archivo txt contiene el nombre de los perfiles de Instagram a ser consultados.
  + Se deben poner uno, hacer un salto de linea, el siguiente y asi sucesivamente.



Espero sea de interés.  Cualquier duda o aclaración de uso se puede a través de esta plataforma o por los canales de contacto que aparecen en la plataforma.





Instagram Listening usa la API de scrapecreators para llamar diversos datos de perfiles de cuentas de instagram como Nombre, perfil, post y hacer una transcripción de lso mismos.

Los archivos en orden de jerarquía se han creado de la siguiente manera:

+ 1_main_profile_stats.py
  + Lo que hace es obtener los datos básicos de una cuenta de Instagram que se le indique en su código.
+ 2_main_posts.py
  + Lo que hace es obtener el listado y características de los post de la cuenta registrada previamente.
+ 3_main_add_transcriptions.py
  + Lo que hace es leer las url de los post de Instagram y a través del endpoint de transcript obtener su contenido.

## Segunda actualización

A este punto se realizó la creación de dos archivos que permitieran expandir el proceso anterior a muchas cuentas de instagram.
Se crea un archivo llamado perfiles_instagram.csv en donde en cada línea se incluye un perfil de instagram a seguir.  También se crean los siguientes archivos.

+ 4_main_instagram_multiple.py
  + Lo que hace es leer los nombres de los perfiles de instagram, obtener los datos del perfil, luego obtener los posts y su información asociada para luego crear una tabla en csv con esa información.
+ 5_transcript_processor.py
  + Lo que hace es leer la última columna del archivo 4_main.... que corresponde a la transcripción del archivo, mira cuales tienen N/A y si tienen a través de leer su url hace la transcripción y la guarda en un archivo temporal.  Luego de que termina de guardarlas todas incorpora esto en la columna correspondiente.
  
## Tercera actualización

Cree un archivo para descargar los post hasta cierta cantidad de tiempo.

+ 6_historical_data_collector.py
    + Lo que hace es pedir un nombre de usuario y descargar a un archivo los post realizados por esa cuenta en los últimos X días.

## Cuarta actualización

Cree archivo para realizar análisis de los 10 primeros videos, imagenes, carrusel dependiendo el engagement que tiene cada uno por perfil de interés.

+ 7_instagram_base_analysis.py
  + Lo que hace es revisar el archivo base_de_datos_instagram.csv generado anteriormente para reconocer el engagement.
+ 8_mention_finder.py
  + Lo que hace es utilizar el endponit de búsqueda de Google para encontrar todas las referencias a un perfil sin incluirlo. La consulta se estructura asi:
    + site: instagram.com "@perfil" -inurl:"https://www.instagram.com/perfil/" after:YYYY-MM-DD



Hecho por Gabriel Alzate

gabriel.andres.alzate@gmail.com

contacto@datoscol.co

