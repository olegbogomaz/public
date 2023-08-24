RESOURCE_GROUP_NAME="rg-func-dev-canc-001"
STORAGE_ACCOUNT_NAME="rgfuncdevcanc001b734"

# This command assumes you have logged in with az login
HTTP_ENDPOINT=$(az storage account show \
    --resource-group $RESOURCE_GROUP_NAME \
    --name $STORAGE_ACCOUNT_NAME \
    --query "primaryEndpoints.file" --output tsv | tr -d '"')
SMBPATH=$(echo $HTTP_ENDPOINT | cut -c7-${#HTTP_ENDPOINT})
FILE_HOST=$(echo $SMBPATH | tr -d "/")

nc -zvw3 $FILE_HOST 445
