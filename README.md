# A simple DynamoDB client for Node.js

It often requires tons of trial-and-error while writing dynamodb integration. For example, you need to call different methods for partition key, sort key, or other arbitrary attribute. The library handles the complexity behind the scene.

At the `connect` time, the library will scan over all tables associated with your AWS account to know the partition key and sort key of each table. In this way, it is able to route to an appropriate dynamodb command automatically.

## Quick start

    import dotenv from 'dotenv'
    import { connect } from './index.js'
    dotenv.config()

    const db = connect({
      region: 'us-east-1',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      accessKeyId: process.env.AWS_ACCESS_KEY_ID
    })

    await db.insert(
      process.env.USERS_TABLE,
      {
        user_id: '123',
        email: 'user@example.com',
        is_active: true
      }
    )

    const user = await db.query(
      process.env.USERS_TABLE,
      { user_id: '123' },
      true // returns single item?
    )

    const allUsers = await db.query(
      process.env.USERS_TABLE,
      { is_active: true },
      false // returns single item?
    )

    const allUsersOnlyEmail = await db.query(
      process.env.USERS_TABLE,
      { is_active: true },
      false, // returns single item?
      ['email'] // attributes to return
    )

    await db.update(
      process.env.USERS_TABLE,
      { user_id: '123' },
      { is_active: false, address: null }
    )

    await db.delete(
      process.env.USERS_TABLE,
      { user_id: '123' }
    )

