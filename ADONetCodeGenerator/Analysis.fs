module ADONetAnalysis

open System
open System.Data
open Microsoft.Data.SqlClient

open Microsoft.SqlServer.Management.Common
open Microsoft.SqlServer.Management.Smo
open Microsoft.SqlServer.Management.Sdk.Sfc

/// Describes an sql datatype. Currently this merges types with the same dotnet representation.
[<Struct; RequireQualifiedAccess>]
type SqlType =
    | Byte
    | Int16
    | Int32
    | Int64
    | DateTime
    | Guid
    | String
    | Bool
    | ByteArray
    | Double
    | Decimal
    | UserDefinedTableType of name: string
    static member FromSQL(dt: DataType) =
        match dt.SqlDataType with
        | SqlDataType.TinyInt -> Byte
        | SqlDataType.SmallInt -> Int16
        | SqlDataType.Int -> Int32
        | SqlDataType.BigInt -> Int64
        | SqlDataType.DateTime | SqlDataType.DateTime2 -> DateTime
        | SqlDataType.UniqueIdentifier -> Guid
        | SqlDataType.NVarChar
        | SqlDataType.NVarCharMax
        | SqlDataType.VarChar
        | SqlDataType.VarCharMax -> String
        | SqlDataType.Bit -> Bool
        | SqlDataType.Binary
        | SqlDataType.VarBinary
        | SqlDataType.VarBinaryMax -> ByteArray
        | SqlDataType.Float -> Double
        | SqlDataType.Timestamp -> ByteArray
        | SqlDataType.Decimal -> Decimal
        | SqlDataType.UserDefinedTableType ->
            UserDefinedTableType dt.Name
        | x -> failwith("Unknown SQL type: " + x.ToString())
    member t.SqlDbType = // TODO: refine the types here
        match t with
        | Byte -> SqlDbType.TinyInt
        | Int16 -> SqlDbType.SmallInt
        | Int32 -> SqlDbType.Int
        | Int64 -> SqlDbType.BigInt
        | DateTime -> SqlDbType.DateTime2
        | Guid -> SqlDbType.UniqueIdentifier
        | String -> SqlDbType.NVarChar
        | Bool -> SqlDbType.Bit
        | ByteArray -> SqlDbType.VarBinary
        | Double -> SqlDbType.Float
        | UserDefinedTableType _ -> SqlDbType.Udt
        | Decimal -> SqlDbType.Decimal
    member t.IsReferenceType =
        match t with
        | Byte | Int16 | Int32 | Int64 | DateTime | Guid | Bool | Double | Decimal -> false
        | String | ByteArray | UserDefinedTableType _ -> true
    static member FromDotnetType(t: Type) =
        if t = typeof<byte> then Byte
        elif t = typeof<int16> then Int16
        elif t = typeof<int> then Int32
        elif t = typeof<int64> then Int64
        elif t = typeof<DateTime> then DateTime
        elif t = typeof<Guid> then Guid
        elif t = typeof<string> then String
        elif t = typeof<bool> then Bool
        elif t = typeof<byte[]> then ByteArray
        elif t = typeof<double> then Double
        elif t = typeof<System.Decimal> then Decimal
        else failwith("Unknown dotnet type: " + t.ToString())

/// An SqlType with annotations: name and nullability
type NamedType(name: string, sqlType: SqlType, nullable: bool) =
    let sqlParameterName = "@" + name
    member _.Name = name
    member _.SqlParameterName = sqlParameterName
    member _.SqlType = sqlType
    member _.Nullable = nullable
    static member FromSql(name: string, dt: DataType, nullable: bool) =
        NamedType(name, SqlType.FromSQL(dt), nullable)
    static member FromParameter(paramName: string, dt: DataType, nullable: bool) =
        let name =
            let withoutAt = paramName.TrimStart('@')
            // convert to camelCase since tooling sometimes converts parameter names to PascalCase. Empty paramName is invalid so exception will be thrown.
            Char.ToLowerInvariant(withoutAt.[0]).ToString() + withoutAt.[1..]
        NamedType(name, SqlType.FromSQL(dt), nullable)
    static member FromDbCol(dbCol: Common.DbColumn) =
        let allowDbNull = if dbCol.AllowDBNull.HasValue then dbCol.AllowDBNull.Value else failwith "AllowDbNull has no value" // false
        let name = if dbCol.ColumnName |> String.IsNullOrEmpty then "Value" else dbCol.ColumnName
        NamedType(name, SqlType.FromDotnetType(nonNull dbCol.DataType), allowDbNull)

type UserDefinedTableT(schemaName: string, name: string, cols: NamedType list) =
    member _.SchemaName = schemaName
    member _.Name = name
    member _.Cols = cols
    static member FromUserDefinedTableType(udt: UserDefinedTableType) =
        let cols =
            [
                for col in udt.Columns do
                    yield NamedType.FromSql(col.Name, col.DataType, col.Nullable)
            ]
        UserDefinedTableT(udt.Schema, udt.Name, cols)

[<RequireQualifiedAccess>]
type Return =
    | None
    | Single of SqlType * isNullable: bool
    | Table of NamedType list * multi: bool
    member t.NamedTypes =
        match t with
        | None -> List.empty
        | Single(sqlp, nullable) -> [ NamedType("Value", sqlp, nullable) ]
        | Table(namedTypes, _) -> namedTypes
    member t.RequiresConstructor =
        match t with
        | None | Single _ -> false
        | Table _ -> true

type CommandTy =
    | UDF
    | StoredProc
    | TableGetter
    | SqlCommand of sql: string

type TypedUDF(schemaName: string, name: string, parameters: NamedType list, returns: Return) =
    member _.SchemaName = schemaName
    member _.Name = name
    member _.SqlQualifiedName = $"[{schemaName}].[{name}]"
    member _.Parameters = parameters
    member _.Returns = returns
    static member FromUserDefinedFunction(udf: UserDefinedFunction) =
        let returns =
            let returnTypes() =
                [
                    for col in udf.Columns do
                        yield NamedType.FromSql(col.Name, col.DataType, col.Nullable)
                ]
            match udf.FunctionType with
            | UserDefinedFunctionType.Scalar ->
                Return.Single(SqlType.FromSQL(udf.DataType), true)
            | UserDefinedFunctionType.Inline ->
                Return.Table(returnTypes(), true)
            | UserDefinedFunctionType.Table ->
                Return.Table(returnTypes(), true)
            | UserDefinedFunctionType.Unknown -> failwith("Unknown function type")
        let parameters =
            [
                for p in udf.Parameters do
                    let isNullable = p.DefaultValue.ToLower() = "null"
                    yield NamedType.FromParameter(p.Name, p.DataType, isNullable)
            ]
        TypedUDF(udf.Schema, udf.Name, parameters, returns)

type TypedSqlCommand(name: string, command: string, parameters: NamedType list, returns: Return) =
    member _.Name = name
    member _.Command = command
    member _.Parameters = parameters
    member _.Returns = returns

    static member FromSql(conn: SqlConnection, name: string, sql: string) =
        let parameters: NamedType list = failwith "not implemented" // Perhaps use sp_describe_undeclared_parameters
        let ret = TypedSqlCommand.GetReturns(conn, sql, CommandType.Text, parameters)
        TypedSqlCommand(name, sql, parameters, ret)

    /// Gets return columns from an sql command, for use in typing stored procedures and text-based sql commands.
    static member GetReturns(conn: SqlConnection, sql: string, commandType: CommandType, parameters: NamedType list) =
        use command = new SqlCommand(sql, conn, CommandType = commandType)
        for p in parameters do
            let parameter =
                match p.SqlType with
                | SqlType.UserDefinedTableType udttName ->
                    SqlParameter(p.SqlParameterName, SqlDbType.Structured, TypeName = udttName)
                | _ ->
                    SqlParameter(p.SqlParameterName, p.SqlType.SqlDbType)
            command.Parameters.Add(parameter) |> ignore
        let reader = command.ExecuteReader(CommandBehavior.SchemaOnly)
        let returnCols = reader.GetColumnSchema() |> Seq.toList |> List.map NamedType.FromDbCol
        match returnCols with
        | [] -> Return.None
        | _ -> Return.Table(returnCols, true)

type TypedStoredProcedure(schemaName: string, name: string, parameters: NamedType list, returns: Return) =
    member _.SchemaName = schemaName
    member _.Name = name
    member _.SqlQualifiedName = $"[{schemaName}].[{name}]"
    member _.Parameters = parameters
    member _.Returns = returns
    static member FromStoredProcedure(conn: SqlConnection, sp: StoredProcedure) =
        let parameters =
            [
                for p in sp.Parameters do
                    let isNullable = p.DefaultValue.ToLower() = "null"
                    yield NamedType.FromParameter(p.Name, p.DataType, isNullable)
            ]
        let ret =
            TypedSqlCommand.GetReturns(conn, sp.Name, CommandType.StoredProcedure, parameters)

        TypedStoredProcedure(sp.Schema, sp.Name, parameters, ret)

type TypedTable(schemaName: string, name: string, columns: NamedType list) =
    member _.SchemaName = schemaName
    member _.Name = name
    member _.Columns = columns
    static member FromTable(table: Table) =
        let columns =
            [
                for col in table.Columns do
                    yield NamedType.FromSql(col.Name, col.DataType, col.Nullable)
            ]
        TypedTable(table.Schema, table.Name, columns)

type Command(name: string, qualifiedName: string, parameters: NamedType seq, returns: Return, commandType: CommandTy) =
    member _.Name = name
    member _.QualifiedName = qualifiedName
    member _.Parameters = parameters
    member _.Returns = returns
    member _.CommandType = commandType
    static member FromTypedUDF(udf: TypedUDF) =
        Command(udf.Name, udf.SqlQualifiedName, udf.Parameters, udf.Returns, CommandTy.UDF)
    static member FromTypedStoredProcedure(sp: TypedStoredProcedure) =
        Command(sp.Name, sp.SqlQualifiedName, sp.Parameters, sp.Returns, CommandTy.StoredProc)
    static member GetterFromTypedTable(tt: TypedTable) =
        let sqlQualifiedName = $"[{tt.SchemaName}].[{tt.Name}]"
        Command(tt.Name, sqlQualifiedName, [], Return.Table(tt.Columns, true), CommandTy.TableGetter)

type DbInfo(udts: UserDefinedTableT list, udfs: TypedUDF list, tsps: TypedStoredProcedure list, tableGetters: TypedTable list) =
    member _.UserDefinedTableTypes = udts
    member _.UserDefinedFunctions = udfs
    member _.StoredProcedures = tsps
    member _.TableGetters = tableGetters
    static member Get(designTimeConn: string, designTimeServer: string) =
        let connectionStringBuilder = SqlConnectionStringBuilder(designTimeConn)
        let server = Server(ServerConnection(designTimeServer))
        let db = Database(server, connectionStringBuilder.InitialCatalog)
        Console.WriteLine("Server version:" + db.ServerVersion.ToString())
        use conn = new SqlConnection(designTimeConn)
        conn.Open()
        let isRelevant(row: DataRow) =
            let schema = row.["Schema"] :?> string
            not(schema = "tmp" || schema = "old" || schema = "sys" || schema = "INFORMATION_SCHEMA")
        let udts =
            let objects = db.EnumObjects(DatabaseObjectTypes.UserDefinedTableTypes)
            [
                for row in objects.Rows do
                    let udt = server.GetSmoObject(Urn(row.["Urn"] :?> string)) :?> UserDefinedTableType
                    yield UserDefinedTableT.FromUserDefinedTableType(udt)
            ]
            |> List.sortBy(fun udtt -> udtt.Name)
        let udfs =
            let objects = db.EnumObjects(DatabaseObjectTypes.UserDefinedFunction)
            [
                for row in objects.Rows do
                    if isRelevant row then
                        let udf = server.GetSmoObject(Urn(row.["Urn"] :?> string)) :?> UserDefinedFunction
                        if not(udf.Name.StartsWith("fn_")) then // exclude system functions like "fn_diagramobjects"
                            yield TypedUDF.FromUserDefinedFunction(udf)
            ]
            |> List.sortBy(fun udf -> udf.Name)
        let tsps =
            let objects = db.EnumObjects(DatabaseObjectTypes.StoredProcedure)
            [
                for row in objects.Rows do
                    if isRelevant row then
                        let sp = server.GetSmoObject(Urn(row.["Urn"] :?> string)) :?> StoredProcedure
                        if not(sp.Name.StartsWith("sp_")) then // exclude system stored procedures like "sp_upgraddiagrams"
                            yield TypedStoredProcedure.FromStoredProcedure(conn, sp)
            ]
            |> List.sortBy(fun sp -> sp.Name)
        let tableGetters =
            let objects = db.EnumObjects(DatabaseObjectTypes.Table)
            [
                for row in objects.Rows do
                    if isRelevant row then
                        let table = server.GetSmoObject(Urn(row.["Urn"] :?> string)) :?> Table
                        if table.Name <> "sysdiagrams" then
                            yield TypedTable.FromTable(table)
            ]
            |> List.sortBy(fun t -> t.Name)
        let schemas =
            let udtSchemas = udts |> List.map(fun udtt -> udtt.SchemaName)
            let udfSchemas = udfs |> List.map(fun udf -> udf.SchemaName)
            let spSchemas = tsps |> List.map(fun sp -> sp.SchemaName)
            let tableSchemas = tableGetters |> List.map(fun t -> t.SchemaName)
            udtSchemas @ udfSchemas @ spSchemas @ tableSchemas |> List.distinct
        schemas |> List.map(fun schema ->
            let udts' = udts |> List.filter(fun udtt -> udtt.SchemaName = schema)
            let udfs' = udfs |> List.filter(fun udf -> udf.SchemaName = schema)
            let tsps' = tsps |> List.filter(fun sp -> sp.SchemaName = schema)
            let tableGetters' = tableGetters |> List.filter(fun t -> t.SchemaName = schema)
            schema, DbInfo(udts', udfs', tsps', tableGetters'))