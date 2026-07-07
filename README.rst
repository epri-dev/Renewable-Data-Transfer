Renewable Data Transfer (RENEWXfer)
==================================

Renewable Data Transfer (RENEWXfer) is an EPRI-developed Python application that
enables secure, standardized extraction, staging, and transfer of renewable
energy system data to external systems via SFTP.

The application supports three EPRI renewable benchmarking platforms:

* **SUPER** – Solar Performance and Reliability Benchmarking
* **LEAP** – Wind Performance Benchmarking
* **BEST** – BESS Benchmarking

RENEWXfer provides configuration-driven execution and auditable delivery of
performance and tracker data across all supported platforms.

---

Purpose and Scope
-----------------

RENEWXfer provides a controlled and standardized mechanism for transferring
renewable operational data. It is designed to support EPRI data collection,
integration, and governance objectives through:

* Secure, auditable delivery of time-series data
* Configurable support for multiple renewable platforms (SUPER, LEAP, BEST)
* Standardized workflows for data extraction and transfer
* Support for multiple data historian sources (PI, Canary)
* Alignment with EPRI cybersecurity and data governance practices

This repository contains the core application, configuration templates, and
supporting artifacts required to deploy and operate RENEWXfer.

---

Key Capabilities
----------------

* Secure SFTP transfer using SSH key- or password-based authentication
* Configurable execution across SUPER, LEAP, and BEST platforms
* Support for multiple data historian sources (PI Historian, Canary)
* Channel list–driven data extraction and tag mapping
* Time- and size-based partitioning of output data files
* Automated generation, compression, and transfer of execution logs
* Validation scripts for verifying PI tag connectivity
* Modular Python architecture for extensibility and maintainability

---

Repository Structure
--------------------

::

    RENEWXfer/
    ├── Main.py                         Primary execution entry point
    ├── validate_LEAP.py               LEAP PI tag validation script
    ├── validate_SUPER.py              SUPER PI tag validation script
    ├── requirements.txt               Python dependency definitions
    ├── Pipfile                        Pipenv dependency management
    ├── constants.env                  Environment configuration file
    ├── Channel_List/                  Channel lists and tag mappings
    │   ├── Tag_mapping_list_SUPER.csv
    │   ├── Tag_mapping_list_LEAP.csv
    │   └── Tag_mapping_list_BEST.csv
    ├── Functions/                     Core functional modules
    │   ├── PI/                        PI Historian data source modules
    │   ├── Canary/                    Canary data source modules
    │   ├── Archive/                   Archived/legacy modules
    │   └── Test/                      Test validation scripts
    ├── Log_Files/                     Execution and transfer logs
    │   ├── SUPER/                     SUPER platform logs
    │   │   └── Trackers/              SUPER tracker-specific logs
    │   ├── LEAP/                      LEAP platform logs
    │   └── BEST/                      BEST platform logs
    ├── File_Staging/                  Staged output data files
    │   ├── SUPER/                     SUPER staged data
    │   │   └── Trackers/              SUPER tracker staged data
    │   ├── LEAP/                      LEAP staged data
    │   └── BEST/                      BEST staged data
    └── SSH_KEYS/                      SSH private keys for authentication

---

System Requirements
-------------------

* Python 3.8 or later
* Windows or Linux operating system with SFTP capability
* Network access to the configured data historian (PI Server or Canary API)
* Network access to the configured SFTP endpoint (if SFTP is enabled)
* Authorized SSH private key or SFTP credentials for authentication

Install dependencies using::

    pip install -r requirements.txt

Alternatively, Pipenv can be used for dependency management::

    pipenv install

---

Configuration
-------------

Application behavior is controlled via the ``constants.env`` file. This file
defines execution parameters, platform selection, data historian source, and
transfer settings.

Data Historian Source
~~~~~~~~~~~~~~~~~~~~~

* ``DATA_HISTORIAN`` – Set to ``PI`` for PI Historian or ``Canary`` for Canary.
  Determines which module set under ``Functions/`` is loaded at runtime.

Platform Toggles
~~~~~~~~~~~~~~~~

* ``SUPER`` – Enable SUPER data processing (1 or 0)
* ``LEAP`` – Enable LEAP data processing (1 or 0)
* ``BEST`` – Enable BEST data processing (1 or 0)

Multiple platforms can be enabled simultaneously.


Platform-Specific Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each platform has its own set of parameters:

* ``CHANNEL_LIST_SUPER`` / ``CHANNEL_LIST_LEAP`` / ``CHANNEL_LIST_BEST`` – Channel list filename for data extraction
* ``DATA_FILE_MAX_LENGTH_SUPER`` / ``DATA_FILE_MAX_LENGTH_LEAP`` / ``DATA_FILE_MAX_LENGTH_BEST`` – Maximum number of days per output file
* ``RAW_DATA_INTERVAL_SUPER`` / ``RAW_DATA_INTERVAL_LEAP`` / ``RAW_DATA_INTERVAL_BEST`` – Data extraction interval (in minutes)
* ``REMOTE_UPLOAD_FOLDER_SUPER`` / ``REMOTE_UPLOAD_FOLDER_LEAP`` / ``REMOTE_UPLOAD_FOLDER_BEST`` – Remote SFTP upload directory
* ``CHANNEL_LIST_VERSION_FLAG_SUPER`` – Channel list version flag for SUPER (0 for old version)
* ``PERFORMANCE_ONLY`` – Process only performance data, excluding tracker data (1 or 0; SUPER only)

Tracker-specific parameters (SUPER only):

* ``DATA_FILE_MAX_LENGTH_TRACKERS`` – Maximum days per tracker output file
* ``RAW_DATA_INTERVAL_TRACKERS`` – Tracker data extraction interval (in minutes)
* ``REMOTE_UPLOAD_FOLDER_TRACKERS`` – Remote SFTP directory for tracker files

All referenced files (channel lists, SSH keys) must be present in their
respective directories prior to execution.

---

Execution
---------

Run the application using::

    python Main.py

Execution behavior is determined by the configuration in ``constants.env``:

* **Platform selection** – Which platforms (SUPER, LEAP, BEST) are enabled
* **Data historian** – PI Historian or Canary data source
* **Workflow mode** – Performance-only or combined performance + tracker (SUPER)
* **SFTP transfer** – Enabled or disabled per execution

For each enabled platform, RENEWXfer will:

1. Load the appropriate channel list from ``Channel_List/``
2. Extract time-series data from the configured data historian
3. Stage output files in ``File_Staging/<platform>/``
4. Transfer files to the remote SFTP endpoint (if SFTP is enabled)
5. Compress and upload execution logs

---

Logging and Audit
-----------------

RENEWXfer generates logs to support traceability and operational auditing.
Logs are organized by platform:

* ``Log_Files/SUPER/`` – SUPER performance data logs
* ``Log_Files/SUPER/Trackers/`` – SUPER tracker-specific logs
* ``Log_Files/LEAP/`` – LEAP data logs
* ``Log_Files/BEST/`` – BEST data logs

Each platform directory also contains an ``SFTP_Logs.log`` file for SFTP
transfer audit trails.

At the end of execution, log files are compressed and transferred to the
configured SFTP destination.

---

Disclaimer
----------

This software is provided for authorized use under applicable EPRI agreements.
Use, modification, and distribution are subject to EPRI policies and
contractual terms.