# Crear base de datos siata para usar en local, y agregar usuario

CREATE DATABASE siata CHARACTER SET UTF8;
use siata;
CREATE USER siata_Consulta@localhost IDENTIFIED BY 'si@t@64512_C0nsult4';
GRANT ALL PRIVILEGES ON siata.* TO siata_Consulta@localhost;
FLUSH PRIVILEGES;

# ir al directorio hidrologiaweb/
virtualenv -p python3 . #verificar que sea python 3.5 o superior # verificar qu$
# para instalar dependencies
pip install -r requirements.txt

