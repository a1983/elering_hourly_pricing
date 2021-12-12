"""Config flow for elering_hourly_pricing."""
import voluptuous as vol

from typing import Any, Dict, Optional

from homeassistant import config_entries
from homeassistant.core import callback

from . import CONF_NAME, UI_CONFIG_SCHEMA
from .const import DOMAIN


class TariffSelectorConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle config flow for `elering_hourly_pricing`."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        """Get the options flow for this handler."""
        return EEOptionsFlowHandler(config_entry)

    async def async_step_user(self, user_input: Optional[Dict[str, Any]] = None):
        """Invoked when a user initiates a flow via the user interface."""
        if user_input is not None:
            self._abort_if_unique_id_configured()
            return self.async_create_entry(title=user_input[CONF_NAME], data=user_input)

        return self.async_show_form(step_id="user", data_schema=UI_CONFIG_SCHEMA)

    async def async_step_import(self, import_info):
        """Handle import from config file."""
        return await self.async_step_user(import_info)


class EEOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle EE options."""

    def __init__(self, config_entry):
        """Initialize options flow."""
        self.config_entry = config_entry

    async def async_step_init(self, user_input=None):
        """Manage the options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        # Fill options with entry data
        schema = vol.Schema({})
        return self.async_show_form(step_id="init", data_schema=schema)
