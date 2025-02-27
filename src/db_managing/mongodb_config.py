from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import logging
import certifi

@dataclass
class MongoDBConfig:
    """Configuration class for MongoDB connections."""
    host: str
    port: int
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    auth_source: str = "admin"
    auth_mechanism: str = "SCRAM-SHA-256"
    enable_auth: bool = True  # New flag to control authentication
    logger: Optional[logging.Logger] = None
    application_name: str = "MongoDBManager"
    min_pool_size: int = 5
    max_pool_size: int = 20
    connect_timeout_ms: int = 30000
    server_selection_timeout_ms: int = 30000
    socket_timeout_ms: int = 30000
    max_idle_time_ms: int = 600000
    enable_ssl: bool = False
    ssl_cert_reqs: str = "CERT_NONE"
    replica_set: Optional[str] = None
    read_preference: str = "primary"
    write_concern: Dict[str, Any] = field(default_factory=lambda: {"w": 1, "j": True})
    retry_writes: bool = True
    retry_reads: bool = True
    connection_options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.host:
            raise ValueError("Host cannot be empty")

        valid_ssl_certs = ["CERT_REQUIRED", "CERT_OPTIONAL", "CERT_NONE"]
        if self.ssl_cert_reqs not in valid_ssl_certs:
            raise ValueError(f"Invalid SSL certificate requirements. Must be one of: {valid_ssl_certs}")

        # Ensure write concern has at least a 'w' value
        if isinstance(self.write_concern, dict) and "w" not in self.write_concern:
            self.write_concern["w"] = 1

    def get_connection_string(self) -> str:
        """Generate a MongoDB connection string from configuration."""
        auth_part = f"{self.user}:{self.password}@" if self.enable_auth and self.user and self.password else ""
        connection_string = f"mongodb://{auth_part}{self.host}:{self.port}/{self.database}"
        params = [
            f"authSource={self.auth_source}" if self.enable_auth and self.auth_source else None,
            f"authMechanism={self.auth_mechanism}" if self.enable_auth and self.auth_mechanism else None,
            f"appName={self.application_name}" if self.application_name else None,
            f"replicaSet={self.replica_set}" if self.replica_set else None,
            f"readPreference={self.read_preference}" if self.read_preference else None,
            "retryWrites=true" if self.retry_writes else None,
            "retryReads=true" if self.retry_reads else None
        ]
        params = [p for p in params if p]
        if params:
            connection_string += "?" + "&".join(params)
        return connection_string

    def get_client_options(self) -> Dict[str, Any]:
        """Generate MongoDB client options dictionary from configuration."""
        client_options = {
            "minPoolSize": self.min_pool_size,
            "maxPoolSize": self.max_pool_size,
            "connectTimeoutMS": self.connect_timeout_ms,
            "serverSelectionTimeoutMS": self.server_selection_timeout_ms,
            "socketTimeoutMS": self.socket_timeout_ms,
            "maxIdleTimeMS": self.max_idle_time_ms,
            "appName": self.application_name,
            "retryWrites": self.retry_writes,
            "retryReads": self.retry_reads,
            "w": self.write_concern.get("w", 1),
            "journal": self.write_concern.get("j", True),
        }

        # Add SSL configuration if enabled
        if self.enable_ssl:
            client_options.update({
                "ssl": True,
                "tlsCAFile": certifi.where(),
                "tlsAllowInvalidCertificates": self.ssl_cert_reqs=="CERT_NONE",
            })

        # Add replica set if specified
        if self.replica_set:
            client_options["replicaSet"] = self.replica_set

        # Set read preference if specified
        if self.read_preference:
            read_pref_map = {
                "primary": "primary",
                "primaryPreferred": "primaryPreferred",
                "secondary": "secondary",
                "secondaryPreferred": "secondaryPreferred",
                "nearest": "nearest"
            }
            if self.read_preference in read_pref_map:
                client_options["readPreference"] = read_pref_map[self.read_preference]

        # Add any additional custom options
        client_options.update(self.connection_options)

        return client_options