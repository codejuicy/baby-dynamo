# A simple DynamoDB client for Node.js

It often requires tons of trial-and-error while writing dynamodb integration. For example, you need to call different methods for partition key, sort key, or other arbitrary attribute. The library handles the complexity behind the scene.

At the initialization time, the library will scan over all tables associated with your AWS account to know the partition key and sort key of each table. In this way, it is able to route to an appropriate dynamodb command automatically.

## Quick start

    import dynamodb from 'dynamodb-client'
    
    const user = await dynamodb.query(
      process.env.USERS_TABLE,
      { email: body.email },
      true // returns single item?
    )

    const all_users = await dynamodb.query(
      process.env.USERS_TABLE,
      { is_active: true },
      false // returns single item?
    )

    await dynamodb.update(
      process.env.USERS_TABLE,
      { user_id: 123 },
      { is_active: false, address: null }
    )
