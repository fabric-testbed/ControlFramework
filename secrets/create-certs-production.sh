#!/bin/bash

set -o nounset \
    -o errexit \
    -o verbose \
    -o xtrace

PASS='abcdefgh'

# Generate CA Key
#openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 -subj '/CN=kafkacat.test.fabric.io/OU=TEST/O=FABRIC/L=ChapelHill/S=NC/C=US' -passin pass:$PASS -passout pass:$PASS


# Kafkacat
openssl genrsa -des3 -passout "pass:$PASS" -out kafkacat.client.key 1024
openssl req -passin "pass:$PASS" -passout "pass:$PASS" -key kafkacat.client.key -new -out kafkacat.client.req -subj '/CN=kafkacat.test.fabric.io/OU=TEST/O=FABRIC/L=ChapelHill/S=NC/C=US'
openssl x509 -req -CA ca-cert -CAkey ca-key -in kafkacat.client.req -out kafkacat-ca1-signed.pem -days 9999 -CAcreateserial -passin "pass:$PASS"



for i in schemaregistry
do
	echo $i
	# Create keystores
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=$i.test.fabric.io, OU=TEST, O=FABRIC, L=ChapelHill, S=NC, C=US" \
				 -keystore kafka.$i.keystore.jks \
				 -keyalg RSA \
				 -storepass $PASS \
				 -keypass $PASS

	# Create CSR, sign the key and import back into keystore
	keytool -keystore kafka.$i.keystore.jks -alias $i -certreq -file $i.csr -storepass $PASS -keypass $PASS

	openssl x509 -req -CA ca-cert -CAkey ca-key -in $i.csr -out $i-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:$PASS

	keytool -keystore kafka.$i.keystore.jks -alias CARoot -import -file ca-cert -storepass $PASS -keypass $PASS

	keytool -keystore kafka.$i.keystore.jks -alias $i -import -file $i-ca1-signed.crt -storepass $PASS -keypass $PASS

	# Create truststore and import the CA cert.
	keytool -keystore kafka.$i.truststore.jks -alias CARoot -import -file ca-cert -storepass $PASS -keypass $PASS

  echo "$PASS" > ${i}_sslkey_creds
  echo "$PASS" > ${i}_keystore_creds
  echo "$PASS" > ${i}_truststore_creds
done
