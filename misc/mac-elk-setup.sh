if [[ $(command -v brew) == "" ]]; then
  echo "Installing Homebrew"
  /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
else
  echo "Homebrew is installed"
fi

declare -a dependencies=("elasticsearch"
                         "logstash"
                         "kibana")

echo "Checking requirements to install ELK stack"

for dependency in ${dependencies[*]}
do
  if brew ls --version $dependency > /dev/null; then
    echo "[OK] $dependency is installed"
  else
    echo "[WARN] $dependency is not installed. We will now install $dependency"
    brew install $dependency
  fi;
done;

echo "Your machine is now ready with ELK stack."
echo "Configure the corresponding services using brew services to have them as background services"
echo "[NOTICE] Configure Kibana from the default Kibana location /usr/local/etc/kibana/kibana.yml"
