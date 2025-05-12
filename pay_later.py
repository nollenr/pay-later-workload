import datetime as dt
import psycopg
import random
import time
import csv
import sys
from itertools import islice

class Pay_later:
    def __init__(self, args: dict):
        self.user_id_chunks = [  
            "00000000-0000-0000-0000-000000000000",
            "0cc8d86f-24dd-4300-8664-6c37926e9979",
            "1994ba62-59ac-4700-8cca-5d312cd62390",
            "2660baa9-a7dc-4000-9330-5d54d3ee0825",
            "332e3ce5-b174-4800-9997-1e72d8ba63b0",
            "3ffd3cfe-4627-4c00-9ffe-9e7f2313c5a1",
            "4ccb5144-f07f-4400-a665-a8a2783ff992",
            "599798b4-62ed-4c00-accb-cc5a317686de",
            "6662c13d-ee1f-4400-b331-609ef70fb1c5",
            "732d25e0-cef0-4000-b996-92f067782839",
            "7ffa28d4-5578-4000-bffd-146a2abc6f8a",
            "8cc78ead-26c1-4000-8663-c7569360c850",
            "99969d4c-5243-4000-8ccb-4ea6292198dd",
            "a6652268-e438-4000-9332-9134721c0f2e",
            "b331eaae-8524-4000-9998-f55742920042",
            "bffb23df-cc1a-4000-9ffd-91efe60d40a6",
            "ccc7b187-e170-4800-a663-d8c3f0b82cc2",
            "d99550d5-9178-4000-acca-a86ac8bbfefb",
            "e6640aa7-d17a-4800-b332-0553e8bd7b6d",
            "f33264c9-d6c2-4000-b999-3264eb61582f",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        self.list_size = 10000
        self.execution_counter = 0
        self.user_index = 0
        self.account_index = 0
        self.accountevent_index = 0
        self.ledger_index = 0
        self.ACCOUNTEVENTS_PER_ACCOUNT = 5
        self.LEDGERS_PER_ACCOUNT = 3
        self.POLL_INTERVAL = 3

    # the setup() function is executed only once
    # when a new executing thread is started.
    # Also, the function is a vector to receive the excuting threads's unique id and the total thread count
    def setup(self, conn: psycopg.Connection, id: int, total_thread_count: int):
        def read_tsv(path, limit=None):
            with open(path, newline='') as f:
                reader = csv.reader(f, delimiter='\t')
                if limit is not None:
                    return list(islice(reader, limit))
                else:
                    return list(reader)

        with conn.cursor() as cur:
            print(
                f"My thread ID is {id}. The total count of threads is {total_thread_count}"
            )
            print(cur.execute(f"select version()").fetchone()[0])

        data_block = random.randint(0, 19)  # This will determine the range of user id's that will be used.  hopefully each thread will use a different block of id's
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT billing_email_id
                FROM "user"
                WHERE id BETWEEN %s AND %s
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (
                    self.user_id_chunks[data_block],
                    self.user_id_chunks[data_block + 1],
                    self.list_size
                ),
            )
            self.user_billing_email_ids = [row[0] for row in cur.fetchall()]  
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id
                FROM account a, "user" u
                WHERE a.user_id = u.id
                AND u.id BETWEEN %s AND %s
                ORDER BY RANDOM()
                LIMIT %s
                """,
                (
                    self.user_id_chunks[data_block],
                    self.user_id_chunks[data_block + 1],
                    self.list_size
                ),
            )
            self.account_ids = [row[0] for row in cur.fetchall()]

            self.users = read_tsv('user.data.tsv', 10)
            self.accounts = read_tsv('account.data.tsv', 10)
            self.accountevents = read_tsv('accountevent.data.tsv', 1000)
            self.ledgers = read_tsv('ledger.data.tsv', 1000)

        # The following section of code allows all of the threads
        # to complete the setup section before moving on to the
        # loop().   This allows me to use the final statistics
        # rather than computing them from the individual output.


        #   CREATE TABLE public.dbworkload_syncup (
        #       thread INT8 NOT NULL,
        #       thread_starting_loop BOOL NULL DEFAULT false,
        #       CONSTRAINT dbworkload_syncup_pkey PRIMARY KEY (thread ASC)
        #   )

        with conn.cursor() as cur:
            # Insert thread ID into sync table
            print(f"Thread {id} is inserting a row in the table dbworkload_syncup")
            cur.execute(
                "INSERT INTO dbworkload_syncup (thread) VALUES (%s) ON CONFLICT DO NOTHING",
                (id,)
            )
            conn.commit()

        # Poll for total rows to match total threads
        while True:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM dbworkload_syncup")
                count = cur.fetchone()[0]
            if count >= total_thread_count:
                with conn.cursor() as cur:
                    print(f"Thread {id} is setting its status to 'starting_loop'")
                    cur.execute(
                        "UPDATE dbworkload_syncup SET thread_starting_loop = true WHERE thread = %s",
                        (id,)
                    )
                conn.commit()
                break  # All threads have reached this point
            time.sleep(self.POLL_INTERVAL)

        if id == 0:
            while True:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM dbworkload_syncup where thread_starting_loop = true")
                    count = cur.fetchone()[0]
                if count >= total_thread_count:
                    print(f"Thread 0 is removing all rows from the dbworkload_syncup table.  ")
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM dbworkload_syncup")
                    conn.commit()
                    break  # All threads have reached this point
                time.sleep(1)

        print(f"✅ Thread {id} detected {count}/{total_thread_count} threads. Proceeding.")


    # the loop() function returns a list of functions
    # that dbworkload will execute, sequentially.
    # Once every func has been executed, loop() is re-evaluated.
    # This process continues until dbworkload exits.
    def loop(self):
        self.execution_counter += 1
        return [
            self.get_customer_by_email,
            self.get_account_events,
            self.get_account_status,
            self.get_account_and_events_1yr,
            self.get_ledger_details,
            self.get_ledger_total,
            self.get_merchant_info,
            self.get_chargebacks,
            self.get_ledger_balances_by_year,
            self.put_user_and_details,
        ]

    #####################
    # Utility Functions #
    #####################

    ######################
    # Workload Functions #
    ######################

    def get_customer_by_email(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.id, a.loan_amount, a.paid_amount, a.interest_rate
                FROM   "user" u, account a 
                WHERE u.billing_email_id = %s
                AND a.user_id = u.id
                """,
                (
                    self.user_billing_email_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_account_events(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT downpayment_amount,
                       incentive_amount,
                       interest_amount
                FROM accountevent
                WHERE account_id = %s
                """,
                (
                    self.account_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_account_status(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT u.dob_year,
                       u.dob_month,
                       u.dob_day,
                       a.loan_owner,
                       a.is_down_payment_captured,
                       a.down_payment_type,
                       sum(e.principal_amount),
                       sum(e.interest_amount),
                       sum(e.incentive_amount)
                FROM "user" u,
                     ACCOUNT a,
                             accountevent e
                WHERE u.billing_email_id = %s
                  AND a.user_id = u.id
                  AND e.account_id = a.id
                GROUP BY u.dob_year,
                         u.dob_month,
                         u.dob_day,
                         a.loan_owner,
                         a.is_down_payment_captured,
                         a.down_payment_type
                """,
                (
                    self.user_billing_email_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_account_and_events_1yr(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.ari,
                       a.type,
                       a.merchant_ari,
                       e.reversal_account_id,
                       e.merchant_fee_billing_strategy
                FROM ACCOUNT a,
                             accountevent e
                WHERE a.id = %s
                  AND a.id = account_id
                  AND e.account_id = a.id
                  AND e.effective BETWEEN NOW() - INTERVAL '5 years' AND NOW()
                """,
                (
                    self.account_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_ledger_details(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT balance_type,
                       amount
                FROM ledger
                WHERE account_id = %s
                """,
                (
                    self.account_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_ledger_total(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sum(amount)
                FROM ledger a
                WHERE account_id = %s
                  AND effective < now()
                  AND VERSION =
                    (SELECT max(VERSION)
                     FROM ledger b
                     WHERE b.account_id = a.account_id
                       AND b.mate_entry_id = a.mate_entry_id)
                """,
                (
                    self.account_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_merchant_info(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.merchant_ari,
                       a.merchant_dispute_fee_revenue,
                       l.txn_type
                FROM "user" u,
                     ACCOUNT a,
                             ledger l
                WHERE u.billing_email_id = %s
                  AND a.user_id = u.id
                  AND l.account_id = a.id
                  AND a.interest_amount > 100
                """,
                (
                    self.user_billing_email_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_chargebacks(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sum(e.amount)
                FROM ACCOUNT a,
                             accountevent e
                WHERE a.id = %s
                  AND e.account_id = a.id
                  AND e.is_chargeback > 0
                  AND e.effective BETWEEN NOW() - INTERVAL '1 year' AND NOW()
                """,
                (
                    self.account_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def get_ledger_balances_by_year(self, conn: psycopg.Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(YEAR
                               FROM l.effective) AS effective_year,
                       SUM(l.amount) AS total_amount
                FROM "user" u,
                     ACCOUNT a,
                             accountevent e,
                             ledger l
                WHERE u.billing_email_id = %s
                  AND a.user_id = u.id
                  AND e.account_id = a.id
                  AND l.account_id = a.id
                GROUP BY effective_year
                ORDER BY effective_year
                """,
                (
                    self.user_billing_email_ids[self.execution_counter % self.list_size],
                ),
            ).fetchall()

    def put_user_and_details(self, conn: psycopg.Connection):
        total_start = time.time()
        with conn.cursor() as cur:
            # Insert a single user (round robin)
            user_row = self.users[self.user_index % len(self.users)]
            # print(user_row[6])
            new_billing_email_id = random.randint(1000, 1102770198321)  # known gap in billing_email_id
            # print(new_billing_email_id)
            self.user_index += 1
            start = time.time()
            # setting column affirm_score_version = 'load-test'
            cur.execute(
                f"""
                INSERT INTO "user" (ari, created, account_ari, status, billing_name_id, billing_phone_number_id, billing_email_id, billing_address_id, billing_instrument_id, dob_year, dob_month, dob_day, ssn_last4_crypt_ari, affirm_score, affirm_score_last_updated, tos_version, profile_thumbnail_url, timezone, timezone_offset, updated, credit_disclosure_version, last_credit_check, ssn_crypt_ari, cohort_id, affirm_score_version, identity_match, credit_report_source, payveris_user_id, billing_email_enabled, billing_sms_enabled, checkout_confirm_counter, mobile_pin_reset_attempts, txn_counter, collections_disabled_reason, welcome_email_flow_origin, last_login_time, share_modal_state, mailing_address_id, label, account_recovery_status, parent_user_ari)
                VALUES ({', '.join(['%s'] * len(user_row))})
                RETURNING id
                """,
                user_row[:6] + [new_billing_email_id] + user_row[7:24] + ['load-test'] + user_row[25:]
            )
            user_id = cur.fetchone()[0]
            # print(f"🕒 User insert time: {time.time() - start:.4f} sec")

            # Insert a single account (round robin)
            account_row = self.accounts[self.account_index % len(self.accounts)]
            self.account_index += 1
            start = time.time()
            # setting column loan_funding_source = 'load-test'
            cur.execute(
                f"""
                INSERT INTO account (created, updated, ari, user_id, merchant_ari, type, principal_receivable, receivable_auth_hold, principal_receivable_hold, user_credit_expense, interest_receivable_hold, interest_revenue, interest_receivable, merchant_dispute_fee_revenue, principal_writeoff_expense, interest_writeoff_expense, merchant_fee_hold, payable, payable_auth_hold, merchant_credit_expense, merchant_fee_revenue, payable_hold, reconciliation, paid_amount, refunded_amount, cashin, cashout, last_interest_accrual, capture_amount, foregone_interest, merchant_fee_refund, credited_amount, apr, interest_amount, principal_amount, reversed_amount, interest_cap, refund_interest_rebate, payment_amount, paid_account_id, pending_interest_payment, pending_interest_receivable, pending_interest_revenue, pending_principal_payment, loan_amount, maturation_date, charged_off_at, interest_chargeoff, principal_chargeoff, loan_funding_source, loan_owner, version, merchant_receivable_invoice, incentive_amount, payment_incentive_expense, accrual_method, interest_rate, interest_refunded_amount, merchant_fee_billing_strategy, user_interest_payable, merchant_refund_agreement, down_payment_amount, down_payment_receivable, down_payment_receivable_chargeoff, refund_voided, refund_voided_offset, split_captured, split_captured_offset, downpayment_refunded, principal_reversed, interest_reversed, down_payment_receivable_hold, down_payment_receivable_hold_offset, is_down_payment_captured, down_payment_type, country_code)
                VALUES ({', '.join(['%s'] * len(account_row))})
                RETURNING id
                """,
                account_row[:3] + [user_id] + account_row[4:49] + ['load-test'] + account_row[50:]
            )
            account_id = cur.fetchone()[0]
            # print(f"🕒 Account insert time: {time.time() - start:.4f} sec")

            # Insert accountevent rows (round robin)
            start = time.time()
            for _ in range(self.ACCOUNTEVENTS_PER_ACCOUNT):
                row = self.accountevents[self.accountevent_index % len(self.accountevents)]
                self.accountevent_index += 1
                # setting column txn_group = 'load-test'
                cur.execute(
                    f"""
                    INSERT INTO accountevent (created, updated, type, account_id, last_entry_id, txn_group, new_loan_owner, new_loan_funding_source, amount, dispute_fee, dispute_resolution, downpayment_amount, effective, mdr, payment_account_id, reversal_account_id, version, merchant_fee_billing_strategy, incentive_amount, interest_amount, principal_amount, interest_refunded, interest_refunded_amount, is_chargeback, bankruptcy_petition_date, refund_event_type, down_payment_type)
                    VALUES ({', '.join(['%s'] * len(row))})
                    """,
                    row[:3] + [account_id] + row[4:5] + ['load-test'] + row[6:]
                )
            # print(f"🕒 Accountevent inserts time: {time.time() - start:.4f} sec")

            # Insert ledger rows (round robin)
            start = time.time()
            for _ in range(self.LEDGERS_PER_ACCOUNT):
                row = self.ledgers[self.ledger_index % len(self.ledgers)]
                self.ledger_index += 1
                # setting column txn_group = 'load-test'
                cur.execute(
                    f"""
                    INSERT INTO ledger (created, updated, mate_entry_id, account_id, txn_group, txn_type, balance_type, amount, version, effective)
                    VALUES ({', '.join(['%s'] * len(row))})
                    """,
                    row[:3] + [account_id] + ['load-test'] + row[5:]
                )
            # print(f"🕒 Ledger inserts time: {time.time() - start:.4f} sec")

        conn.commit()
        # print("✅ Inserted 1 user, 1 account, accountevent {}, ledger {}".format(self.ACCOUNTEVENTS_PER_ACCOUNT, self.LEDGERS_PER_ACCOUNT))
        # print(f"🕒 Total time: {time.time() - total_start:.4f} sec")


'''
# Quick random generators reminder

# random string of 25 chars
self.random_str(25),

# random int between 0 and 100k
random.randint(0, 100000),

# random float with 2 decimals
round(random.random()*1000000, 2)

# now()
dt.datetime.utcnow()

# random timestamptz between certain dates,
# expressed as unix ts
dt.datetime.fromtimestamp(random.randint(1655032268, 1759232268))

# random UUID
uuid4()

# random bytes
size = 12
random.getrandbits(8 * size).to_bytes(size, "big")

'''