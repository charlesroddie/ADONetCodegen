namespace GeneratedADONET

open Microsoft.Data.SqlClient
open System

/// Describes an SqlConnection with optioal SqlTransaction for passing into an SqlCommand.
type ISqlConnection =
    abstract Connection: SqlConnection
    abstract Transaction: SqlTransaction option

type SqlConnWithTransaction(conn: SqlConnection, tran: SqlTransaction) =
    member _.CommitAsync() = tran.CommitAsync()
    member _.RollbackAsync() = tran.RollbackAsync()
    interface ISqlConnection with
        member _.Connection = conn
        member _.Transaction = Some tran
    interface IDisposable with
        member _.Dispose() =
            conn.Dispose()
            tran.Dispose()

type SqlConn(conn: SqlConnection) =
    interface ISqlConnection with
        member _.Connection = conn
        member _.Transaction = None
    interface IDisposable with member _.Dispose() = conn.Dispose()