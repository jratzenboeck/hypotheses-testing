var db = require('./db');

module.exports = {
    getSubscriptionDataForUser: getSubscriptionDataForUser
};

function getSubscriptionDataForUser(data, cb) {
    findSubscriptionData(data.user_id, data.tracker_id, function(err, result) {
        if (err) {
            return cb(err, null);
        }
        cb(err, result[0]);
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
                user_id: 1, tracker_id: 1, created_at: 1, _id: 0,
                additional_service_ids: 1, start_data: 1, payment_plan_interval: '$payment_plan.interval',
                payment_plan_type: '$payment_plan.type'
            }
        }
    ];
    return db.aggregate(db.getTractiveDbConnection(), 'ppl_subscriptions', pipeline, cb);
}
