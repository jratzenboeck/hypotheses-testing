var db = require('./db');

module.exports = {
    getSubscriptionDataForUser: getSubscriptionDataForUser
};

function getSubscriptionDataForUser(data, cb) {
    findSubscriptionData(data.user_id, data.tracker_id, function(err, result) {
        if (err) {
            return cb(err, null);
        }
        if (!!result && result.length === 1) {
            cb(err, result[0]);
        } else {
            cb(err, {
                tracker_id: data.tracker_id,
                created_at: -1, tractive_care: -1,
                payment_plan_interval: -1,
                payment_plan_type: -1
            });
        }
    });
}

function findSubscriptionData(userId, trackerId, cb) {
    var pipeline = [
        {
            '$match': {
                user_id: userId,
                tracker_id: trackerId
            }
        },
        {
            '$lookup': {
                from: 'ppl_payment_plans',
                localField: 'payment_plan_id',
                foreignField: '_id',
                as: 'payment_plan'
            }
        },
        {
            '$unwind': '$payment_plan'
        },
        {
            '$project': {
                tracker_id: 1, created_at: 1, _id: 0,
                tractive_care: {
                    $cond: {
                        if: { $gt: [ {$size: '$additional_service_ids'}, 0 ] },
                        then: 'yes', else: 'no'
                    }
                },
                payment_plan_interval: '$payment_plan.interval',
                payment_plan_type: '$payment_plan.type'
            }
        },
        {
            '$limit': 1
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), 'ppl_subscriptions', pipeline, cb);
}
