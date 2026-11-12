db.comments.find(
  { $expr: { $lt: [ { $strLenCP: "$content" }, 5 ] } }
).limit(5)

db.comments.aggregate([
  {
    $match: {
      $expr: { $lt: [ { $strLenCP: "$content" }, 5 ] }
    }
  },
  {
    $project: {
      userImage: 0,
      score: 0,
      thumbsUpCount: 0,
      reviewCreatedVersion: 0,
      replyContent: 0,
      repliedAt: 0
    }
  }
])

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

