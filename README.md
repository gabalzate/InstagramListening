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
