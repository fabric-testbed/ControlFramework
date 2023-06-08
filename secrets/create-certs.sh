#!/bin/bash

set -o nounset \
    -o errexit \
    -o verbose \
    -o xtrace

# Generate CA key
openssl req -new -x509 -keyout snakeoil-ca-1.key -out snakeoil-ca-1.crt -days 365 -subj '/CN=ca1.test.fabric.io/OU=TEST/O=FABRIC/L=ChapelHill/S=NC/C=US' -passin pass:fabric -passout pass:fabric
cp snakeoil-ca-1.crt snakeoil-ca-1-copy.crt
# openssl req -new -x509 -keyout snakeoil-ca-2.key -out snakeoil-ca-2.crt -days 365 -subj '/CN=ca2.test.fabric.io/OU=TEST/O=FABRIC/L=ChapelHill/S=NC/C=US' -passin pass:fabric -passout pass:fabric

# kafkacat1
openssl genrsa -des3 -passout "pass:fabric" -out kafkacat1.client.key 2048
openssl req -passin "pass:fabric" -passout "pass:fabric" -key kafkacat1.client.key -new -out kafkacat1.client.req -subj '/CN=kafkacat1.test.fabric.io/OU=TEST/O=FABRIC/L=ChapelHill/S=NC/C=US'
openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -in kafkacat1.client.req -out kafkacat1-ca1-signed.pem -days 9999 -CAcreateserial -passin "pass:fabric"

openssl genrsa -des3 -passout "pass:fabric" -out kafkacat2.client.key 2048
openssl req -passin "pass:fabric" -passout "pass:fabric" -key kafkacat2.client.key -new -out kafkacat2.client.req -subj '/CN=kafkacat2.test.fabric.io/OU=TEST/O=FABRIC/L=ChapelHill/S=NC/C=US'
openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -in kafkacat2.client.req -out kafkacat2-ca1-signed.pem -days 9999 -CAcreateserial -passin "pass:fabric"


for i in broker1 schemaregistry producer consumer
do
	echo $i
	# Create keystores
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=$i.test.fabric.io, OU=TEST, O=FABRIC, L=ChapelHill, S=NC, C=US" \
				 -keystore kafka.$i.keystore.jks \
				 -keyalg RSA \
				 -storepass fabric \
				 -keypass fabric

	# Create CSR, sign the key and import back into keystore
	keytool -keystore kafka.$i.keystore.jks -alias $i -certreq -file $i.csr -storepass fabric -keypass fabric

	openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -in $i.csr -out $i-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:fabric

	keytool -keystore kafka.$i.keystore.jks -alias CARoot -import -file snakeoil-ca-1.crt -storepass fabric -keypass fabric -noprompt

	keytool -keystore kafka.$i.keystore.jks -alias $i -import -file $i-ca1-signed.crt -storepass fabric -keypass fabric

	# Create truststore and import the CA cert.
	keytool -keystore kafka.$i.truststore.jks -alias CARoot -import -file snakeoil-ca-1.crt -storepass fabric -keypass fabric -noprompt

  echo "fabric" > ${i}_sslkey_creds
  echo "fabric" > ${i}_keystore_creds
  echo "fabric" > ${i}_truststore_creds
done
