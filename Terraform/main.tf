provider "azurerm" {
  subscription_id = "Your subscription ID"
  tenant_id       = "Your tenant ID"

  features {}
}

resource "azurerm_resource_group" "kiva_rg1" {
  name     = "kiva_rg"
  location = "northeurope"
}

resource "azurerm_storage_account" "kiva_storage" {
  name                     = "kivastorageacc" 
  resource_group_name      = azurerm_resource_group.kiva_rg.name
  location                 = azurerm_resource_group.kiva_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "kiva_container" {
  name                  = "kivabronze"
  storage_account_name  = azurerm_storage_account.kiva_storage.name
  container_access_type = "private"
}
