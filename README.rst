Renewable Data Transfer (RENEWXfer)
==================================

RENEWXfer is an EPRI-developed Python application that enables secure,
standardized extraction, staging, and transfer of renewable energy system
data to EPRI via SFTP.

The application supports three EPRI renewable benchmarking platforms:

* **SUPER®** – Solar Performance and Reliability Benchmarking
* **LEAP®** – Wind Performance Benchmarking
* **BEST®** – BESS Benchmarking

---

Purpose and Scope
-----------------

RENEWXfer provides a controlled and standardized mechanism for transferring
renewable operational data. It is designed to support EPRI data collection,
integration, and governance objectives through:

* Secure, auditable delivery of time-series data
* Configurable support for multiple renewable platforms (SUPER®, LEAP®, BEST®)
* Standardized workflows for data extraction and transfer
* Support for multiple data historian sources (OSIsoft PI, Canary)
* Alignment with EPRI cybersecurity and data governance practices

---

Key Capabilities
----------------

* Secure SFTP transfer using SSH key- or password-based authentication
* Configurable execution across SUPER®, LEAP®, and BEST® platforms
* Support for OSIsoft PI Historian and Canary data sources
* Channel list–driven data extraction and tag mapping
* Automatic catch-up for new tags added to an existing plant
* Time- and size-based partitioning of output data files
* Checkpoint-based resumption — re-runs safely pick up where they left off
* Automated compression and SFTP transfer of execution logs
* Validation scripts for verifying PI and Canary tag connectivity

---

Repository Structure
--------------------

::

    RENEWXfer/
    ├── Main.py                         Primary execution entry point
    ├── validate_LEAP.py                LEAP PI tag validation script
    ├── validate_SUPER.py               SUPER PI tag validation script
    ├── requirements.txt                Python dependency definitions
    ├── pyproject.toml                  PEP 517 build and dependency manifest
    ├── constants.env                   Environment configuration (NOT for source control)
    ├── Channel_List/                   Channel lists and tag mapping files
    │   ├── Tag_mapping_list_SUPER.csv
    │   ├── Tag_mapping_list_LEAP.csv
    │   └── Tag_mapping_list_BEST.csv
    ├── Functions/                      Core functional modules
    │   ├── upload_log_files.py         Log archive and SFTP upload utility
    │   ├── PI/                         OSIsoft PI Historian modules
    │   ├── Canary/                     Canary Historian modules
    │   ├── Archive/                    Legacy modules (not used in production)
    │   └── Test/                       Development test scripts (not for production use)
    ├── Log_Files/                      Execution and transfer logs (runtime-generated)
    │   ├── SUPER/
    │   │   └── Trackers/
    │   ├── LEAP/
    │   └── BEST/
    ├── File_Staging/                   Staged output data files (runtime-generated)
    │   ├── SUPER/
    │   │   └── Trackers/
    │   ├── LEAP/
    │   └── BEST/
    └── SSH_KEYS/                       SSH private key storage (NOT for source control)

.. note::

   ``constants.env`` and the ``SSH_KEYS/`` directory contain sensitive
   credentials. Add both to ``.gitignore`` before committing to any repository.

---

System Requirements
-------------------

* Python **3.11** or later
* Network access to the configured data historian (PI Server or Canary API)
* Network access to the EPRI SFTP endpoint (if SFTP transfer is enabled)
* SFTP credentials or SSH private key provided by EPRI

Install dependencies::

    pip install -r requirements.txt

---

Configuration
-------------

All behavior is controlled by the ``constants.env`` file in the project root.
Copy and fill in this file before running the tool. Do not commit it to
source control.

Data Historian
~~~~~~~~~~~~~~

``DATA_HISTORIAN``
    Set to ``PI`` for OSIsoft PI Historian or ``Canary`` for Canary Historian.
    This determines which module set under ``Functions/`` is loaded at runtime.

    For Canary: also set the API server hostname and API token directly in
    ``Functions/Canary/CanaryAPI.py`` (``self.server`` and ``self.apiToken``).

Platform Toggles
~~~~~~~~~~~~~~~~

``SUPER``, ``LEAP``, ``BEST``
    Set to ``1`` to enable, ``0`` to disable. Multiple platforms can run in
    a single execution.

SFTP Settings
~~~~~~~~~~~~~

``SFTP_ENABLED``
    Set to ``1`` to transfer files over SFTP, ``0`` to stage files locally
    without uploading. When disabled, checkpoints are not advanced and staged
    archives are retained.

``USE_PASSWORD`` / ``USE_SSHKEY``
    Set exactly one to ``1`` to select the authentication method.

``SFTP_HOST``, ``SFTP_USERNAME``, ``SFTP_PASSWORD``
    EPRI-provided SFTP connection details.

``SFTP_PRIVATE_KEY``
    Filename of the SSH private key placed in the ``SSH_KEYS/`` directory.

``SFTP_PRIVATE_KEY_PASS``
    Passphrase for the SSH private key, if applicable.

``SLEEP_TIME``, ``MAX_COUNT``
    Retry wait time (seconds) and maximum retry attempts for failed uploads.

Platform-Specific Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For each platform (``SUPER``, ``LEAP``, ``BEST``):

``CHANNEL_LIST_<PLATFORM>``
    Filename of the Excel channel list in ``Channel_List/``.

``DATA_FILE_MAX_LENGTH_<PLATFORM>``
    Maximum span of one output file in days.

``RAW_DATA_INTERVAL_<PLATFORM>``
    Data extraction interval in minutes.

``REMOTE_UPLOAD_FOLDER_<PLATFORM>``
    Remote SFTP directory provided by EPRI.

SUPER®-only parameters:

``PERFORMANCE_ONLY``
    Set to ``1`` to run performance data only (no tracker data).

``CHANNEL_LIST_VERSION_FLAG_SUPER``
    Set to ``0`` for the current channel list format.

``DATA_FILE_MAX_LENGTH_TRACKERS``, ``RAW_DATA_INTERVAL_TRACKERS``
    File length and interval settings for SUPER tracker data.

``REMOTE_UPLOAD_FOLDER_TRACKERS``
    Remote SFTP directory for SUPER tracker files.

---

Execution
---------

Run the application from the project root::

    python Main.py

For each enabled platform, RENEWXfer will:

1. Load the channel list from ``Channel_List/``
2. Read the last checkpoint from ``Log_Files/<platform>/<plant>_log.csv``
3. Extract time-series data from the configured data historian
4. Stage compressed output files in ``File_Staging/<platform>/``
5. Transfer files to the EPRI SFTP endpoint (if ``SFTP_ENABLED=1``)
6. Update the checkpoint log only on confirmed successful upload
7. Compress and transfer all execution logs at the end of the run

Checkpoint behavior
~~~~~~~~~~~~~~~~~~~

RENEWXfer tracks the last successfully uploaded timestamp per tag in a per-plant
CSV log. On re-run it resumes from where the previous run succeeded. If a
transfer fails, the checkpoint is not advanced so data is re-attempted on the
next run. New tags added to a channel list are automatically caught up from
the plant's commissioning date before regular processing continues.

---

Tag Validation
--------------

Before running production transfers, validate that all tags in a channel list
are accessible:

For PI::

    python validate_LEAP.py
    python validate_SUPER.py

These scripts attempt to read each tag from the PI server and report any tags
that are unreachable or return no data.

---

Logging and Audit
-----------------

Logs are organized by platform under ``Log_Files/``:

* ``Log_Files/SUPER/SFTP_Logs.log`` – SUPER® SFTP transfer audit trail
* ``Log_Files/SUPER/Trackers/SFTP_Logs.log`` – SUPER® Tracker SFTP audit trail
* ``Log_Files/LEAP/SFTP_Logs.log`` – LEAP® SFTP transfer audit trail
* ``Log_Files/BEST/SFTP_Logs.log`` – BEST® SFTP transfer audit trail
* ``Log_Files/<platform>/<plant>_log.csv`` – Per-plant upload checkpoint log

At the end of each run, the entire ``Log_Files/`` directory is compressed and
transferred to the SFTP endpoint via ``Functions/upload_log_files.py``.

---

Security Considerations
-----------------------

* ``constants.env`` contains SFTP credentials and must not be committed to
  source control. Add it to ``.gitignore``.
* ``SSH_KEYS/`` contains private key material and must not be committed to
  source control. Add it to ``.gitignore``.
* SFTP host key verification is currently disabled (``cnopts.hostkeys = None``).
  For production environments, configure known-host verification by populating
  ``cnopts.hostkeys`` with the EPRI server's host key.
* The Canary API communicates over HTTPS with certificate verification disabled
  for self-signed certificates. Verify the server certificate independently if
  operating in a strict security environment.

---

Disclaimer
----------

Copyright © Electric Power Research Institute, Inc. (EPRI). All rights reserved.

This software is provided for authorized use under applicable EPRI agreements.
Use, modification, and distribution are subject to EPRI policies and
contractual terms. EPRI makes no warranties, express or implied, regarding
fitness for a particular purpose or the accuracy of results produced by this
software.
