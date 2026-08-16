from __future__ import annotations

import bisect
import hashlib
import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import dotenv_values


REPO = Path(__file__).resolve().parents[1]
R = REPO / "research"

CONTRACT = (
    R
    / "rn1_f21_m6_burned_replay_contract.json"
)

TAPE = (
    R
    / "h2_v2_m2h_tape.jsonl"
)

OUT = (
    R
    / "rn1_f21_m6_trade_side_gate.json"
)

EXPECTED_CONTRACT_SHA = (
    "c653dafcab0cc1a35695ac3bb3863d16"
    "b089efdd20514eb0ec949ec1e03aecbc"
)


def sha256(path: Path) -> str:
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def load_levels(raw) -> dict[int, Decimal]:
    out = {}

    for level in raw or []:

        if (
            not isinstance(
                level,
                (list, tuple),
            )
            or len(level) < 2
        ):
            raise RuntimeError(
                f"bad book level: {level!r}"
            )

        price = int(
            level[0]
        )

        size = Decimal(
            str(level[1])
        )

        if size < 0:
            raise RuntimeError(
                "negative book size"
            )

        if size > 0:
            out[price] = size

    return out


def clean(value):

    if isinstance(
        value,
        Decimal,
    ):
        return str(value)

    if isinstance(
        value,
        dict,
    ):
        return {
            str(k):
                clean(v)

            for k, v
            in value.items()
        }

    if isinstance(
        value,
        (list, tuple),
    ):
        return [
            clean(v)
            for v in value
        ]

    return value


# --------------------------------------------------
# Frozen-input guards
# --------------------------------------------------

if (
    sha256(CONTRACT)
    != EXPECTED_CONTRACT_SHA
):
    raise RuntimeError(
        "REFUSING M6: contract hash changed"
    )


contract = json.loads(
    CONTRACT.read_text(
        encoding="utf-8"
    )
)


if (
    contract["status"]
    !=
    "FROZEN_BURNED_REAL_DATA_REPLAY_VALIDATION"
):
    raise RuntimeError(
        "unexpected M6 contract status"
    )


rows = [
    json.loads(line)

    for line
    in TAPE.read_text(
        encoding="utf-8"
    ).splitlines()

    if line.strip()
]


token_ids = sorted({
    int(row["token_id"])
    for row in rows
})


if len(token_ids) != 18:
    raise RuntimeError(
        "unexpected burned token universe"
    )


minimum_cases = int(
    contract[
        "trade_side_gate"
    ][
        "minimum_comparable_cases"
    ]
)

minimum_fraction = Decimal(
    str(
        contract[
            "trade_side_gate"
        ][
            "minimum_direct_support_fraction"
        ]
    )
)


# --------------------------------------------------
# Read-only DB connection
# --------------------------------------------------

env = dotenv_values(
    REPO / ".env"
)

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="quantlab",
    user="quantlab",
    password=env["PG_PASSWORD"],
    cursor_factory=(
        psycopg2.extras.RealDictCursor
    ),
)

conn.set_session(
    readonly=True,
    autocommit=False,
)


try:

    with conn.cursor() as cur:

        # ------------------------------------------
        # Full authoritative WS snapshots
        # ------------------------------------------

        cur.execute(
            """
            SELECT
                token_id,
                event_time,
                capture_time,
                connection_id,
                ingest_sequence,
                book_generation,
                bids,
                asks

            FROM book_snapshots

            WHERE
                token_id = ANY(%s)
                AND watchlist_rule = 'h2_v2'
                AND source = 'ws_book'
                AND event_time IS NOT NULL
                AND connection_id IS NOT NULL
                AND ingest_sequence IS NOT NULL
                AND book_generation IS NOT NULL

            ORDER BY
                token_id,
                connection_id,
                event_time,
                ingest_sequence
            """,
            (token_ids,),
        )

        books = [
            dict(x)
            for x in cur.fetchall()
        ]


        # ------------------------------------------
        # Explicit trade records
        # ------------------------------------------

        cur.execute(
            """
            SELECT
                token_id,
                event_time,
                capture_time,
                connection_id,
                ingest_sequence,
                side,
                price_mc,
                size

            FROM last_trade_events

            WHERE
                token_id = ANY(%s)
                AND event_time IS NOT NULL
                AND connection_id IS NOT NULL
                AND ingest_sequence IS NOT NULL
                AND side IN (
                    'BUY',
                    'SELL'
                )
                AND price_mc IS NOT NULL
                AND size IS NOT NULL

            ORDER BY
                token_id,
                connection_id,
                event_time,
                ingest_sequence
            """,
            (token_ids,),
        )

        trades = [
            dict(x)
            for x in cur.fetchall()
        ]


    # --------------------------------------------------
    # Group same-token / same-connection streams
    # --------------------------------------------------

    books_by_stream = defaultdict(
        list
    )

    trades_by_stream = defaultdict(
        list
    )


    for book in books:

        key = (
            int(
                book["token_id"]
            ),
            str(
                book[
                    "connection_id"
                ]
            ),
        )

        books_by_stream[
            key
        ].append(
            book
        )


    for trade in trades:

        key = (
            int(
                trade["token_id"]
            ),
            str(
                trade[
                    "connection_id"
                ]
            ),
        )

        trades_by_stream[
            key
        ].append(
            trade
        )


    # --------------------------------------------------
    # Diagnostics / gate counters
    # --------------------------------------------------

    reasons = Counter()

    classification = Counter()

    by_side = {
        "BUY":
            Counter(),

        "SELL":
            Counter(),
    }

    exact_quantity = Counter()

    comparable_examples = []


    for key, stream_trades in (
        trades_by_stream.items()
    ):

        stream_books = (
            books_by_stream.get(
                key,
                [],
            )
        )

        if len(stream_books) < 2:

            reasons[
                "no_two_book_stream"
            ] += len(
                stream_trades
            )

            continue


        # Already DB-ordered, but make this
        # deterministic independently.
        stream_books = sorted(
            stream_books,
            key=lambda x: (
                x["event_time"],
                int(
                    x[
                        "ingest_sequence"
                    ]
                ),
            ),
        )

        stream_trades = sorted(
            stream_trades,
            key=lambda x: (
                x["event_time"],
                int(
                    x[
                        "ingest_sequence"
                    ]
                ),
            ),
        )


        book_times = [
            x["event_time"]
            for x in stream_books
        ]

        trade_times = [
            x["event_time"]
            for x in stream_trades
        ]


        book_time_counts = Counter(
            book_times
        )


        for trade_index, trade in enumerate(
            stream_trades
        ):

            t = trade[
                "event_time"
            ]

            trade_seq = int(
                trade[
                    "ingest_sequence"
                ]
            )


            # --------------------------------------
            # PRE:
            # latest strictly earlier venue-time
            # authoritative ws_book.
            # --------------------------------------

            pre_pos = (
                bisect.bisect_left(
                    book_times,
                    t,
                )
                - 1
            )

            if pre_pos < 0:

                reasons[
                    "no_pre_book"
                ] += 1

                continue


            pre = stream_books[
                pre_pos
            ]

            pre_time = pre[
                "event_time"
            ]


            if (
                book_time_counts[
                    pre_time
                ]
                != 1
            ):

                reasons[
                    "duplicate_pre_book_time"
                ] += 1

                continue


            # Earlier venue-time state must also
            # precede the trade in collector order.
            if (
                int(
                    pre[
                        "ingest_sequence"
                    ]
                )
                >= trade_seq
            ):

                reasons[
                    "pre_sequence_inconsistent"
                ] += 1

                continue


            # --------------------------------------
            # POST:
            #
            # If a ws_book exists at the exact same
            # venue event_time, it is the candidate
            # post-trade authoritative state.
            #
            # Otherwise use the first strictly later
            # venue-time ws_book. No seconds/ms
            # tolerance is introduced.
            # --------------------------------------

            exact_left = (
                bisect.bisect_left(
                    book_times,
                    t,
                )
            )

            exact_right = (
                bisect.bisect_right(
                    book_times,
                    t,
                )
            )


            if (
                exact_right
                > exact_left
            ):

                if (
                    exact_right
                    - exact_left
                    != 1
                ):

                    reasons[
                        "multiple_exact_post_books"
                    ] += 1

                    continue

                post = stream_books[
                    exact_left
                ]


            else:

                post_pos = (
                    bisect.bisect_right(
                        book_times,
                        t,
                    )
                )

                if (
                    post_pos
                    >= len(
                        stream_books
                    )
                ):

                    reasons[
                        "no_post_book"
                    ] += 1

                    continue


                post = stream_books[
                    post_pos
                ]


                # For a strictly later venue event,
                # collector sequence must also be
                # strictly later.
                if (
                    int(
                        post[
                            "ingest_sequence"
                        ]
                    )
                    <= trade_seq
                ):

                    reasons[
                        "post_sequence_inconsistent"
                    ] += 1

                    continue


            post_time = post[
                "event_time"
            ]


            if (
                book_time_counts[
                    post_time
                ]
                != 1
            ):

                reasons[
                    "duplicate_post_book_time"
                ] += 1

                continue


            # --------------------------------------
            # Frozen continuity rules
            # --------------------------------------

            if (
                pre[
                    "book_generation"
                ]
                !=
                post[
                    "book_generation"
                ]
            ):

                reasons[
                    "generation_crossing"
                ] += 1

                continue


            # connection already fixed by grouping.


            # --------------------------------------
            # Exactly one trade in:
            #
            #     (pre event_time, post event_time]
            #
            # This prevents a multi-trade transition
            # from being attributed to one trade.
            # --------------------------------------

            lo = bisect.bisect_right(
                trade_times,
                pre_time,
            )

            hi = bisect.bisect_right(
                trade_times,
                post_time,
            )


            if (
                hi - lo
                != 1
            ):

                reasons[
                    "multi_trade_bracket"
                ] += 1

                continue


            if (
                stream_trades[
                    lo
                ]["ingest_sequence"]
                !=
                trade[
                    "ingest_sequence"
                ]
            ):

                reasons[
                    "candidate_not_unique_trade"
                ] += 1

                continue


            reasons[
                "clean_causal_bracket"
            ] += 1


            # --------------------------------------
            # Compare full authoritative level state
            # at the exact reported trade price.
            # --------------------------------------

            price = int(
                trade[
                    "price_mc"
                ]
            )

            quantity = Decimal(
                str(
                    trade[
                        "size"
                    ]
                )
            )


            if quantity <= 0:

                reasons[
                    "nonpositive_trade_size"
                ] += 1

                continue


            pre_bids = load_levels(
                pre[
                    "bids"
                ]
            )

            pre_asks = load_levels(
                pre[
                    "asks"
                ]
            )

            post_bids = load_levels(
                post[
                    "bids"
                ]
            )

            post_asks = load_levels(
                post[
                    "asks"
                ]
            )


            pre_bid = pre_bids.get(
                price,
                Decimal("0"),
            )

            pre_ask = pre_asks.get(
                price,
                Decimal("0"),
            )

            post_bid = post_bids.get(
                price,
                Decimal("0"),
            )

            post_ask = post_asks.get(
                price,
                Decimal("0"),
            )


            bid_decrease = max(
                Decimal("0"),
                pre_bid - post_bid,
            )

            ask_decrease = max(
                Decimal("0"),
                pre_ask - post_ask,
            )


            side = trade[
                "side"
            ]


            if side == "BUY":

                direct_decrease = (
                    ask_decrease
                )

                inverse_decrease = (
                    bid_decrease
                )

                direct_pre = (
                    pre_ask
                )

                inverse_pre = (
                    pre_bid
                )


            elif side == "SELL":

                direct_decrease = (
                    bid_decrease
                )

                inverse_decrease = (
                    ask_decrease
                )

                direct_pre = (
                    pre_bid
                )

                inverse_pre = (
                    pre_ask
                )


            else:

                raise RuntimeError(
                    f"unexpected side {side}"
                )


            direct_supported = (
                direct_pre > 0
                and
                direct_decrease > 0
            )

            inverse_supported = (
                inverse_pre > 0
                and
                inverse_decrease > 0
            )


            if (
                direct_supported
                and
                not inverse_supported
            ):

                label = (
                    "DIRECT"
                )

                if (
                    direct_decrease
                    == quantity
                ):

                    exact_quantity[
                        "direct_exact_quantity"
                    ] += 1

                else:

                    exact_quantity[
                        "direct_nonexact_quantity"
                    ] += 1


            elif (
                inverse_supported
                and
                not direct_supported
            ):

                label = (
                    "INVERSE"
                )

                if (
                    inverse_decrease
                    == quantity
                ):

                    exact_quantity[
                        "inverse_exact_quantity"
                    ] += 1

                else:

                    exact_quantity[
                        "inverse_nonexact_quantity"
                    ] += 1


            elif (
                direct_supported
                and
                inverse_supported
            ):

                label = (
                    "AMBIGUOUS"
                )


            else:

                label = (
                    "NEUTRAL"
                )


            classification[
                label
            ] += 1

            by_side[
                side
            ][
                label
            ] += 1


            if (
                label in (
                    "DIRECT",
                    "INVERSE",
                )
                and
                len(
                    comparable_examples
                ) < 20
            ):

                comparable_examples.append({
                    "token_id":
                        key[0],

                    "side":
                        side,

                    "trade_event_time":
                        t.isoformat(),

                    "trade_ingest_sequence":
                        trade_seq,

                    "price_mc":
                        price,

                    "trade_size":
                        quantity,

                    "pre_event_time":
                        pre_time.isoformat(),

                    "post_event_time":
                        post_time.isoformat(),

                    "book_generation":
                        pre[
                            "book_generation"
                        ],

                    "pre_bid":
                        pre_bid,

                    "post_bid":
                        post_bid,

                    "pre_ask":
                        pre_ask,

                    "post_ask":
                        post_ask,

                    "bid_decrease":
                        bid_decrease,

                    "ask_decrease":
                        ask_decrease,

                    "classification":
                        label,
                })


    # --------------------------------------------------
    # Frozen gate
    # --------------------------------------------------

    direct = int(
        classification[
            "DIRECT"
        ]
    )

    inverse = int(
        classification[
            "INVERSE"
        ]
    )

    comparable = (
        direct
        + inverse
    )


    if comparable > 0:

        direct_fraction = (
            Decimal(
                direct
            )
            /
            Decimal(
                comparable
            )
        )

    else:

        direct_fraction = (
            Decimal("0")
        )


    gate_pass = (
        comparable
        >= minimum_cases

        and

        direct_fraction
        >= minimum_fraction

        and

        direct
        > inverse
    )


    result = {
        "study":
            "RN1-F21-M6-TRADE-SIDE-GATE",

        "status":
            (
                "PASS"
                if gate_pass
                else "FAIL"
            ),

        "contract_sha256":
            sha256(CONTRACT),

        "method":
            (
                "Deterministic same-token, same-connection, "
                "same-generation authoritative ws_book "
                "causal bracketing with no arbitrary "
                "time-distance tolerance."
            ),

        "input": {
            "tokens":
                len(
                    token_ids
                ),

            "ws_books":
                len(
                    books
                ),

            "trades":
                len(
                    trades
                ),
        },

        "frozen_thresholds": {
            "minimum_comparable_cases":
                minimum_cases,

            "minimum_direct_support_fraction":
                minimum_fraction,

            "direct_support_must_exceed_inverse":
                True,
        },

        "classification": {
            "direct":
                direct,

            "inverse":
                inverse,

            "ambiguous":
                int(
                    classification[
                        "AMBIGUOUS"
                    ]
                ),

            "neutral":
                int(
                    classification[
                        "NEUTRAL"
                    ]
                ),

            "comparable":
                comparable,

            "direct_support_fraction":
                direct_fraction,
        },

        "by_reported_side": {
            side: {
                "direct":
                    int(
                        counts[
                            "DIRECT"
                        ]
                    ),

                "inverse":
                    int(
                        counts[
                            "INVERSE"
                        ]
                    ),

                "ambiguous":
                    int(
                        counts[
                            "AMBIGUOUS"
                        ]
                    ),

                "neutral":
                    int(
                        counts[
                            "NEUTRAL"
                        ]
                    ),
            }

            for side, counts
            in by_side.items()
        },

        "quantity_sensitivity":
            dict(
                exact_quantity
            ),

        "exclusions":
            dict(
                reasons
            ),

        "candidate_mapping_if_pass": {
            "BUY":
                (
                    "aggressive BUY "
                    "consumes resting asks"
                ),

            "SELL":
                (
                    "aggressive SELL "
                    "consumes resting bids"
                ),
        },

        "sample_comparable_cases":
            comparable_examples,

        "analysis_boundary": {
            "F19_read":
                False,

            "F20_read":
                False,

            "prospective_data_read":
                False,

            "strategy_signal_read":
                False,

            "markout_calculated":
                False,

            "winner_used":
                False,

            "settlement_used":
                False,

            "pnl_calculated":
                False,
        },
    }


    OUT.write_text(
        json.dumps(
            clean(result),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


    print()
    print(
        "========================================"
    )
    print(
        "F21-M6 TRADE-SIDE GATE"
    )
    print(
        "========================================"
    )

    print(
        "status:",
        result["status"],
    )

    print(
        "input trades:",
        len(trades),
    )

    print(
        "clean causal brackets:",
        int(
            reasons[
                "clean_causal_bracket"
            ]
        ),
    )

    print(
        "direct support:",
        direct,
    )

    print(
        "inverse support:",
        inverse,
    )

    print(
        "ambiguous:",
        int(
            classification[
                "AMBIGUOUS"
            ]
        ),
    )

    print(
        "neutral:",
        int(
            classification[
                "NEUTRAL"
            ]
        ),
    )

    print(
        "comparable cases:",
        comparable,
    )

    print(
        "direct support fraction:",
        str(
            direct_fraction
        ),
    )

    print()
    print(
        "--- by reported side ---"
    )

    for side in (
        "BUY",
        "SELL",
    ):

        x = by_side[
            side
        ]

        print(
            side,
            "| direct",
            int(x["DIRECT"]),
            "| inverse",
            int(x["INVERSE"]),
            "| ambiguous",
            int(x["AMBIGUOUS"]),
            "| neutral",
            int(x["NEUTRAL"]),
        )


    print()
    print(
        "--- exact-quantity sensitivity ---"
    )

    print(
        "direct exact:",
        int(
            exact_quantity[
                "direct_exact_quantity"
            ]
        ),
    )

    print(
        "direct nonexact:",
        int(
            exact_quantity[
                "direct_nonexact_quantity"
            ]
        ),
    )

    print(
        "inverse exact:",
        int(
            exact_quantity[
                "inverse_exact_quantity"
            ]
        ),
    )

    print(
        "inverse nonexact:",
        int(
            exact_quantity[
                "inverse_nonexact_quantity"
            ]
        ),
    )


    print()
    print(
        "--- frozen criteria ---"
    )

    print(
        "minimum comparable:",
        minimum_cases,
    )

    print(
        "minimum direct fraction:",
        str(
            minimum_fraction
        ),
    )

    print(
        "direct > inverse required: YES"
    )


    print()
    print(
        "--- boundary ---"
    )

    print(
        "arbitrary timing tolerance: NO"
    )

    print(
        "F19 read: NO"
    )

    print(
        "F20 read: NO"
    )

    print(
        "signal read: NO"
    )

    print(
        "markout: NO"
    )

    print(
        "settlement/winner: NO"
    )

    print(
        "PnL: NO"
    )

    print(
        "artifact SHA:",
        sha256(OUT),
    )


finally:

    conn.rollback()
    conn.close()
