Renewable Data Transfer (RENEWXfer)
==================================

Renewable Data Transfer (RENEWXfer), also referred to as **SUPERXfer**, is an
EPRI-developed Python application that enables secure, standardized extraction,
staging, and transfer of renewable energy system data to external systems via
SFTP.

The application supports EPRI renewable data workflows by providing
configuration-driven execution and auditable delivery of performance and tracker
data.

---

Purpose and Scope
-----------------

RENEWXfer provides a controlled and standardized mechanism for transferring
renewable operational data. It is designed to support EPRI data collection,
integration, and governance objectives through:

* Secure, auditable delivery of time-series data
* Configurable support for multiple renewable asset data types
* Standardized workflows for data extraction and transfer
* Alignment with EPRI cybersecurity and data governance practices

This repository contains the core application, configuration templates, and
supporting artifacts required to deploy and operate RENEWXfer.

---

Key Capabilities
----------------

* Secure SFTP transfer using SSH key-based authentication
* Configurable execution of performance and tracker data workflows
* Channel list–driven data extraction and mapping
* Time- and size-based partitioning of output data files
* Automated generation and transfer of execution logs
* Modular Python architecture for extensibility and maintainability

---

Repository Structure
--------------------

::

    Renewable-Data-Transfer/
    ├── Main.py                         Primary execution entry point
    ├── requirements.txt               Python dependency definitions
    ├── constants.env                  Environment configuration file
    ├── Channel_List/                  Channel lists and tag mappings
    ├── Functions/                     Core functional modules
    ├── Log_Files/                     Execution and transfer logs
    ├── File_Staging/                  Staged output data files
    ├── SSH_KEYS/                      SSH private keys for authentication
    ├── SUPERXfer_READ_ME.pdf          Detailed SUPERXfer documentation
    └── run_SUPERXfer_instructions.pdf Execution guidance

---

System Requirements
-------------------

* Python 3.8 or later
* Windows or Linux operating system with SFTP capability
* Network access to the configured SFTP endpoint
* Authorized SSH private key for authentication

Install dependencies using::

    pip install -r requirements.txt

---

Configuration
-------------

Application behavior is controlled via the ``constants.env`` file. This file
defines execution parameters, data selection, and transfer settings.

Key parameters include:

* ``CHANNEL_LIST`` – Channel list filename used for data extraction
* ``DATA_FILE_MAX_LENGTH`` – Maximum number of days per output file
* ``RAW_DATA_INTERVAL`` – Data extraction interval (in minutes)
* ``PERFORMANCE_ONLY`` – Enable performance workflow (1 or 0)
* ``TRACKER_ONLY`` – Enable tracker workflow (1 or 0)
* ``SFTP_PRIVATE_KEY`` – SSH private key filename

All referenced files (channel lists, keys) must be present in their respective
directories prior to execution.

---

Execution
---------

Run the application using::

    python Main.py

Execution behavior is determined by configuration flags:

* Performance workflow only
* Tracker workflow only
* Combined execution of both workflows

Output data files are generated in the ``File_Staging/`` directory and then
transferred via SFTP.

---

Logging and Audit
-----------------

RENEWXfer generates logs to support traceability and operational auditing.

Log locations:

* ``Log_Files/`` – General execution logs
* ``Log_Files/Trackers/`` – Tracker-specific logs
* ``Log_Files/SFTP_Logs.log`` – SFTP transfer logs

At the end of execution, log files are compressed and transferred to the
configured SFTP destination.

---

Disclaimer
----------

This software is provided for authorized use under applicable EPRI agreements.
Use, modification, and distribution are subject to EPRI policies and
contractual terms.