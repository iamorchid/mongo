'use strict';

/**
 * Tests periodically killing sessions that are running transactions.
 *
 * @tags: [uses_transactions, assumes_snapshot_transactions]
 */

load('jstests/concurrency/fsm_libs/extend_workload.js');  // for extendWorkload
load('jstests/concurrency/fsm_workloads/multi_statement_transaction_atomicity_isolation.js');

var $config = extendWorkload($config, ($config, $super) => {
    $config.data.retryOnKilledSession = true;

    $config.states.killSession = function killSession(db, collName) {
        let ourSessionWasKilled;
        do {
            ourSessionWasKilled = false;

            try {
                let res = db.adminCommand({refreshLogicalSessionCacheNow: 1});
                if (res.ok === 1) {
                    assertAlways.commandWorked(res);
                } else if (res.code === 18630 || res.code === 18631) {
                    // Refreshing the logical session cache may trigger sharding the sessions
                    // collection, which can fail with 18630 or 18631 if its session is killed while
                    // running DBClientBase::getCollectionInfos() or DBClientBase::getIndexSpecs(),
                    // respectively. This means the collection is not set up, so retry.
                    ourSessionWasKilled = true;
                    continue;
                } else {
                    assertAlways.commandFailedWithCode(res, ErrorCodes.DuplicateKey);
                }

                const sessionToKill = db.getSiblingDB("config").system.sessions.aggregate([
                    {$listSessions: {}},
                    {$match: {"_id.id": {$ne: db.getSession().getSessionId().id}}},
                    {$sample: {size: 1}},
                ]);

                if (sessionToKill.toArray().length === 0) {
                    break;
                }

                const sessionUUID = sessionToKill.toArray()[0]._id.id;
                res = db.runCommand({killSessions: [{id: sessionUUID}]});
                assertAlways.commandWorked(res);
            } catch (e) {
                if (e.code == ErrorCodes.Interrupted || e.code == ErrorCodes.CursorKilled ||
                    e.code == ErrorCodes.CursorNotFound) {
                    // This session was killed when running either listSessions or killSesssions.
                    // We should retry.
                    ourSessionWasKilled = true;
                    continue;
                }

                throw e;
            }
        } while (ourSessionWasKilled);
    };

    $config.transitions = {
        init: {update: 0.9, checkConsistency: 0.1},
        update: {update: 0.8, checkConsistency: 0.1, killSession: 0.1},
        checkConsistency: {update: 0.9, killSession: 0.1},
        killSession: {update: 0.9, checkConsistency: 0.1}
    };

    return $config;
});
