db.comments.aggregate([
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
])


db.comments.find(
  { $expr: { $lt: [ { $strLenCP: "$content" }, 5 ] } }
).limit(5)


db.comments.aggregate([
  {
    $group: {
      _id: { 
        $dateToString: { 
          format: "%Y-%m-%d", 
          date: { $toDate: "$at" } 
        } 
      },
      avgRating: { $avg: "$score" }
    }
  },
  {
    $project: {
      _id: 0,
      date: { $dateFromString: { dateString: "$_id" } }, 
      avgRating: 1
    }
  },
  { $sort: { date: 1 } }
])

