## Installation :
1. git clone inscritssellsyversbitrix24etsendpulse (à modifier)
2. cd inscritssellsyversbitrix24etsen4etsendpulse
3. Copier les fichiers anomalies.json, googleaccess.json, retargetingV10.json et config.ini à la main (vim ou nano).
4. sudo docker build -t anomalies .
5. touch access.log
6. crontab -e
7. 20 07 * * * sudo docker run --rm -d --name anomalies anomalies > /home/support/inscritssellsyversbitrix24etsendpulse/access.log
---


## Démarrage :
1. sudo docker run -it --rm --name anomalies anomalies
---


## Mise à jour :
1. cd /home/support/inscritssellsyversbitrix24etsendpulse
2. git pull origin
---