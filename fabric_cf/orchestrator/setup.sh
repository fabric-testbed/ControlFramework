# MIT License
#
# Copyright (c) 2020 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author Komal Thareja (kthare10@renci.org)

# After making modifications to this file, please restart actor to re-read it.

# This file is a sample; to alter a particular value, uncomment it, and set as desired.
# actor will use sane defaults, in the absence of this configuration file.

if [ "$#" -lt 3 ]; then
    echo "Illegal number of parameters"
    exit
fi

if [ "$#" -gt 4 ]; then
    echo "Illegal number of parameters"
    exit
fi

name=$1
neo4jpwd=$2
config=$3


mkdir -p $name/logs $name/pg_data/data $name/pg_data/logs $name/neo4j/data $name/neo4j/imports $name/neo4j/logs $name/pdp/conf $name/pdp/policies
echo $neo4jpwd > $name/neo4j/password
cp fabricTags.OrchestratorTags.xml $name/pdp/policies
cp slice_expiration_template.txt $name/
cp env.template $name/.env
cp $config $name/config.yaml
cp -R nginx $name/

if [ -z $4 ]; then
  cp docker-compose.yml $name/
else
  cp docker-compose-dev.yml $name/docker-compose.yml
  cp -R certs $name/
fi

sed -i "s/orchestrator/$name/g" $name/docker-compose.yml
sed -i "s/orchestrator/$name/g" $name/config.yaml
echo ""
echo ""
echo "Update $name/.env file and volumes SSL certs details for $name container in docker-compose.yml as needed"
echo ""
echo ""
