#! /bin/bash

source $(dirname "$0")/version.sh
# Set default version to develop
APP_VERSION=develop

# Trigger read version
read_version
echo "Version: $APP_VERSION"

create_release() {
    user="rosettadb"
    repo="rosetta"
    token=$TOKEN
    tag="v$APP_VERSION"

    command="curl -s -o release.json -w '%{http_code}' \
         --request POST \
         --header 'Accept: application/vnd.github.v3+json' \
         --header 'Authorization: token ${token}' \
         --header 'content-type: application/json' \
         --data '{\"tag_name\": \"${tag}\", \"name\": \"Release ${tag}\", \"body\":\"Release ${tag}\", \"target_commitish\":\"main\"}' \
         https://api.github.com/repos/$user/$repo/releases"
    http_code=`eval $command`
    if [ $http_code == "201" ]; then
        echo "created release:"
        cat release.json
    else
        echo "create release failed with code '$http_code':"
        cat release.json
        echo "command:"
        echo $command
        return 1
    fi
}

rename_release_files() {
  FILE=$1
  OS=$2

  unzip $FILE.zip
  mv binary-$OS/ $FILE/
  rm -rf $FILE.zip
  zip -r $FILE.zip $FILE
}

upload_custom_release_file() {
    token=$TOKEN
    name=$1
    path=$2
    file="$path$name"

    url=`jq -r .upload_url release.json | cut -d{ -f'1'`
    command="\
      curl -s -o upload.json -w '%{http_code}' \
           --request POST \
           --header 'Accept: application/vnd.github.v3+json' \
           --header 'Authorization: token ${token}' \
           --header 'Content-Type: application/octet-stream' \
           --data-binary @\"${file}\"
           ${url}?name=${name}"
    http_code=`eval $command`
    if [ $http_code == "201" ]; then
        echo "asset $name uploaded:"
        jq -r .browser_download_url upload.json
    else
        echo "upload failed with code '$http_code':"
        cat upload.json
        echo "command:"
        echo $command
        return 1
    fi
}

create_release
cd binary/build
rename_release_files "rosetta-$APP_VERSION-linux-x64" "linux-x64"
rename_release_files "rosetta-$APP_VERSION-mac_aarch64" "mac_aarch64"
rename_release_files "rosetta-$APP_VERSION-mac_x64" "mac_x64"
rename_release_files "rosetta-$APP_VERSION-win_x64" "win_x64"
cd ../..
upload_custom_release_file "rosetta-$APP_VERSION-linux-x64.zip" "binary/build/"
upload_custom_release_file "rosetta-$APP_VERSION-mac_aarch64.zip" "binary/build/"
upload_custom_release_file "rosetta-$APP_VERSION-mac_x64.zip" "binary/build/"
upload_custom_release_file "rosetta-$APP_VERSION-win_x64.zip" "binary/build/"
upload_custom_release_file "cli-$APP_VERSION.jar" "cli/build/libs/"
