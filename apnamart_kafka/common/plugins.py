"""Plugin management system for extensibility."""

import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PluginManager:
    """Manages plugins for various extension points."""

    def __init__(self) -> None:
        """Initialize the plugin manager."""
        self._plugins: Dict[str, Dict[str, Any]] = {}
        self._plugin_instances: Dict[str, Dict[str, Any]] = {}

    def register_plugin(
        self, category: str, name: str, plugin_class: Type[T], **kwargs: Any
    ) -> None:
        """Register a plugin class.

        Args:
            category: Plugin category (e.g., 'serializers', 'monitoring', 'security')
            name: Plugin name
            plugin_class: Plugin class to register
            **kwargs: Additional arguments for plugin initialization
        """
        if category not in self._plugins:
            self._plugins[category] = {}
            self._plugin_instances[category] = {}

        self._plugins[category][name] = {
            "class": plugin_class,
            "kwargs": kwargs,
        }

        logger.debug(f"Registered plugin '{name}' in category '{category}'")

    def get_plugin(self, category: str, name: str) -> Optional[Any]:
        """Get a plugin instance.

        Args:
            category: Plugin category
            name: Plugin name

        Returns:
            Plugin instance or None if not found
        """
        if category not in self._plugin_instances:
            return None

        # Create instance if not already created
        if name not in self._plugin_instances[category]:
            if category in self._plugins and name in self._plugins[category]:
                plugin_info = self._plugins[category][name]
                try:
                    instance = plugin_info["class"](**plugin_info["kwargs"])
                    self._plugin_instances[category][name] = instance
                    logger.debug(
                        f"Created instance of plugin '{name}' in category '{category}'"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to create plugin instance '{name}' in '{category}': {e}"
                    )
                    return None
            else:
                return None

        return self._plugin_instances[category].get(name)

    def list_plugins(self, category: Optional[str] = None) -> Dict[str, List[str]]:
        """List all registered plugins.

        Args:
            category: Optional category to filter by

        Returns:
            Dictionary mapping categories to plugin names
        """
        if category:
            return {category: list(self._plugins.get(category, {}).keys())}

        return {cat: list(plugins.keys()) for cat, plugins in self._plugins.items()}

    def remove_plugin(self, category: str, name: str) -> bool:
        """Remove a plugin.

        Args:
            category: Plugin category
            name: Plugin name

        Returns:
            True if plugin was removed, False if not found
        """
        removed = False

        if category in self._plugins and name in self._plugins[category]:
            del self._plugins[category][name]
            removed = True

        if (
            category in self._plugin_instances
            and name in self._plugin_instances[category]
        ):
            del self._plugin_instances[category][name]
            removed = True

        if removed:
            logger.debug(f"Removed plugin '{name}' from category '{category}'")

        return removed

    def clear_category(self, category: str) -> None:
        """Clear all plugins in a category.

        Args:
            category: Category to clear
        """
        if category in self._plugins:
            del self._plugins[category]

        if category in self._plugin_instances:
            del self._plugin_instances[category]

        logger.debug(f"Cleared all plugins in category '{category}'")

    def clear_all(self) -> None:
        """Clear all plugins."""
        self._plugins.clear()
        self._plugin_instances.clear()
        logger.debug("Cleared all plugins")


# Global plugin manager instance
plugin_manager = PluginManager()
