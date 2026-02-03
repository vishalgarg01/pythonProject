"""
Send test results to Capillary API Tester in a single request.

Run:
  python apitest_update_results.py

Environment overrides:
  APITEST_AUTH      Basic auth header value.
  APITEST_CSRF      CSRF token cookie.
  APITEST_URL       Target endpoint.
  APITEST_RUN_NAME  Run name key under "result" (defaults to DEFAULT_RUN_NAME).
"""

from __future__ import annotations

import os
from typing import Dict, List

import requests

# Default credentials provided by the user; can be overridden via env vars.
DEFAULT_AUTH = os.environ.get(
    "APITEST_AUTH",
    "Basic Q2FwaWxsYXJ5OklVTmhjR2xzYkdGeWVVQTROekl5",
)
DEFAULT_CSRF = os.environ.get("APITEST_CSRF", "hjdeIRDyuH2w1QMrveIbdg86LW2I7ZZv")
DEFAULT_URL = os.environ.get(
    "APITEST_URL", "https://apitester.capillary.in/apitest_app/addResult"
)
DEFAULT_RUN_NAME = "Connect_Plus_Sgcrm_Sanity__2026-01-30_07-30-04"


def test_cases() -> List[Dict[str, str]]:
    """Return the test cases to post (no stored messages)."""
    return [
        {"suite": "Test_Retro_Transaction", "case": "test_retro_transaction_sanity", "status": "Pass", "time": "0:00:34"},
        {"suite": "Test_transaction_e2e", "case": "test_transaction_import_sanity", "status": "Pass", "time": "0:56:49"},
        {"suite": "TestBadgesCreateUpdate", "case": "test_badges_create_update_sanity_skip_for_ushc_crm_skip_for_seacrm", "status": "Pass", "time": "0:57:21"},
        {"suite": "TestCsvToJson", "case": "test_csv2json_sanity_skip_for_ningxia_crm_skip_for_tatacrm_skip_for_devenv_crm", "status": "Pass", "time": "0:57:22"},
        {"suite": "TestCsvToXml", "case": "test_csv_to_xml_sanity", "status": "Pass", "time": "0:57:20"},
        {"suite": "TestCustomerAdd", "case": "test_customer_add_sanity", "status": "Pass", "time": "0:57:09"},
        {"suite": "TestCustomerAddOkFiles", "case": "test_customerAdd_ok_sanity", "status": "Pass", "time": "0:57:13"},
        {"suite": "TestDatabricksJob", "case": "test_databricks_job_sanity", "status": "Pass", "time": "0:10:59"},
        {"suite": "TestDataValidationBlock", "case": "test_data_validation_block_sanity_skip_for_tatacrm_skip_for_ushc_crm", "status": "Pass", "time": "0:57:12"},
        {"suite": "TestIntouchTransactionSanity", "case": "test_intouchTransaction_sanity", "status": "Pass", "time": "0:57:03"},
        {"suite": "TestPullDataFromApi", "case": "test_http_iteration_pull_data_from_api_sanity_skip_for_tatacrm_skip_for_ushc_crm_skip_for_seacrm_skip_for_nightly_cc", "status": "Pass", "time": "0:00:02"},
        {"suite": "TestSftp2SftpMerge", "case": "test_sftp2sftp_merge_sanity", "status": "Pass", "time": "0:57:08"},
        {"suite": "TestSftp2SftpMergeFourFiles", "case": "test_sftp2sftp_merge_four_files_sanity", "status": "Pass", "time": "0:57:08"},
        {"suite": "TestSftp2SftpMergeThreeFiles", "case": "test_sftp2sftp_merge_three_files_sanity", "status": "Pass", "time": "0:57:05"},
        {"suite": "TestSftp2SftpWithHeader", "case": "test_sftp2sftp_with_header_sanity", "status": "Pass", "time": "0:56:53"},
        {"suite": "TestTransactionCustomerAdd", "case": "test_transactionCustomerAdd_sanity", "status": "Pass", "time": "0:56:55"},
        {"suite": "TestTransactionLineItemMerge", "case": "test_transaction_lineItem_merge_sanity_skip_for_ushc_crm", "status": "Pass", "time": "0:56:43"},
        {"suite": "TestTransactionV2AddWithPaymentMode", "case": "test_transaction_v2_add_with_paymentMode_sanity", "status": "Pass", "time": "0:56:43"},
    ]


def build_payload_for_case(run_name: str, tc: Dict[str, str]) -> Dict[str, Dict]:
    """Build a payload for a single test case."""
    return {
        "result": {
            run_name: {
                tc["suite"]: {
                    tc["case"]: {
                        "time": tc["time"],
                        "total": 1,
                        "validataionMessage": "",
                        "screenshot": "",
                        "comments": "",
                        "status": tc["status"],
                    }
                }
            }
        }
    }


def send_results(run_name: str) -> None:
    headers = {
        "Authorization": DEFAULT_AUTH,
        "Content-Type": "application/json",
        "Cookie": f"csrftoken={DEFAULT_CSRF}",
    }

    for tc in test_cases():
        payload = build_payload_for_case(run_name, tc)
        response = requests.post(DEFAULT_URL, headers=headers, json=payload, timeout=60)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:  # pragma: no cover - simple CLI
            print(f"Request failed for {tc['case']} with status {response.status_code}: {exc}")
            print("Response text:\n", response.text)
            raise

        print(f"Sent {tc['case']} -> {response.status_code}")


def main() -> None:
    run_name = os.environ.get("APITEST_RUN_NAME", DEFAULT_RUN_NAME)
    send_results(run_name=run_name)


if __name__ == "__main__":
    main()



