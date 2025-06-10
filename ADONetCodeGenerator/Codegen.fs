module ADONetCodegen
open ADONetAnalysis
open System.Collections.Immutable

[<RequireQualifiedAccess; Struct>]
type FormatParameterOption =
    | Dotnet of includeType: bool * includePrecedingComma: bool
    | Sql

module SqlType =
    let dotnetTypeStr(sqlType: SqlType) =
        match sqlType with
        | SqlType.Byte -> "byte"
        | SqlType.Int16 -> "int16"
        | SqlType.Int32 -> "int"
        | SqlType.Int64 -> "int64"
        | SqlType.DateTime -> "DateTime"
        | SqlType.Guid -> "Guid"
        | SqlType.String -> "string"
        | SqlType.Bool -> "bool"
        | SqlType.ByteArray -> "byte[]"
        | SqlType.Double -> "double"
        | SqlType.Decimal -> "decimal"
        | SqlType.UserDefinedTableType udtName -> $"ImmutableArray<{udtName}>"
    let getterStr(sqlType: SqlType, reader: string, index: int) = // TODO: get these names
        match sqlType with
        | SqlType.Byte -> $"{reader}.GetByte({index})"
        | SqlType.Int16 -> $"{reader}.GetInt16({index})"
        | SqlType.Int32 -> $"{reader}.GetInt32({index})"
        | SqlType.Int64 -> $"{reader}.GetInt64({index})"
        | SqlType.DateTime -> $"{reader}.GetDateTime({index})"
        | SqlType.Guid -> $"{reader}.GetGuid({index})"
        | SqlType.String -> $"{reader}.GetString({index})"
        | SqlType.Bool -> $"{reader}.GetBoolean({index})"
        | SqlType.ByteArray -> $"{reader}.[{index}] :?> byte[]"
        | SqlType.Double -> $"{reader}.GetDouble({index})"
        | SqlType.Decimal -> $"{reader}.GetDecimal({index})"
        | SqlType.UserDefinedTableType udttName ->
            $"(let r = {reader}.GetData({index}) in {udttName}.FromReader(r))"

module NamedType =
    let formatWithType(nt: NamedType) = nt.Name + ": " + SqlType.dotnetTypeStr(nt.SqlType) + (if nt.Nullable then " voption" else "")
    let formatParameters(parameters: NamedType seq, fpo: FormatParameterOption) =
        let strs =
            parameters |> Seq.map(fun p ->
                match fpo with
                | FormatParameterOption.Dotnet(includeTypes, _) -> if includeTypes then formatWithType p else p.Name
                | FormatParameterOption.Sql -> p.SqlParameterName
            )
        match fpo with
        | FormatParameterOption.Dotnet(_, true) ->
            strs |> Seq.map(fun s -> ", " + s) |> String.concat ""
        | _ -> strs |> String.concat ", "
    let printFnWithParameters(name: string, parameters: NamedType seq, fpo: FormatParameterOption) =
        name + "(" + (formatParameters(parameters, fpo)) + ")"

    let formatToDotnetInput(nt: NamedType) =
        if nt.Nullable then
            if nt.SqlType.IsReferenceType then ($"(match {nt.Name} with | ValueSome x -> x :> (obj | null) | ValueNone -> null)")
            else ($"(match {nt.Name} with | ValueSome x -> Nullable(x) | ValueNone -> Nullable())")
        else nt.Name
    let readParameterCode(nt: NamedType, reader: string, position: int) =
        if nt.Nullable then
            $"(if {reader}.IsDBNull({position}) then ValueNone else ValueSome({SqlType.getterStr(nt.SqlType, reader, position)}))"
        else
            $"{SqlType.getterStr(nt.SqlType, reader, position)}"
    let toSqlParameter(nt: NamedType) =
        match nt.SqlType with
        | SqlType.UserDefinedTableType udttName ->
            $"SqlParameter(\"{nt.SqlParameterName}\", Data.SqlDbType.Structured, TypeName = \"{udttName}\", Value = {udttName}.ToDataTable({formatToDotnetInput nt}))"
        | _ -> // primitive type
            $"SqlParameter(\"{nt.SqlParameterName}\", SqlDbType.{nt.SqlType.SqlDbType.ToString()}, Value = {formatToDotnetInput nt})"

module private Helpers =
    let memberExpr(name: string) = "    member _." + name + " = " + name
    let readSingleRow(nts: NamedType list, outputTypeName: string) =
        let inputs = nts |> Seq.mapi(fun i nt -> NamedType.readParameterCode(nt, "reader", i)) |> String.concat ", "
        $"{outputTypeName}({inputs})"

module UserDefinedTableT =
    let fSharpTypeDef(udtt: UserDefinedTableT) =
        let readSingleRow =
            udtt.Cols |> List.mapi(fun i col -> NamedType.readParameterCode(col, "reader", i)) |> String.concat ", "
        let writeRow =
            udtt.Cols |> List.map(fun col -> "row." + col.Name) |> String.concat ", "
        [   yield $"type {udtt.Name}({NamedType.formatParameters(udtt.Cols, FormatParameterOption.Dotnet(true, false))}) ="
            for col in udtt.Cols do
                yield $"    member _.{col.Name} = {col.Name}"
            yield $"    static member FromReader(reader: SqlDataReader) ="
            yield $"        let b = ImmutableArray.CreateBuilder<{udtt.Name}>()"
            yield "        while reader.Read() do"
            yield $"            b.Add({udtt.Name}({readSingleRow}))"
            yield "        reader.Close()"
            yield "        b.ToImmutable()"

            yield $"    static member ToDataTable(rows: ImmutableArray<{udtt.Name}>) ="
            yield "        let dt = new DataTable()" // TODO: think about disposal (even though unimportant https://learn.microsoft.com/en-us/answers/questions/224772/do-i-need-to-use-dispose)
            for col in udtt.Cols do
                let (sqlType: SqlType) = col.SqlType
                yield $"""        dt.Columns.Add("{col.Name}", typeof<{SqlType.dotnetTypeStr sqlType}>) |> ignore"""
            yield "        for row in rows do"
            yield $"            dt.Rows.Add({writeRow}) |> ignore"
            yield "        dt"
        ]

module Command =
    let format(c: Command) =
        let firstLine =
            if c.Returns.RequiresConstructor then
                $"type {NamedType.printFnWithParameters(c.Name, c.Returns.NamedTypes, FormatParameterOption.Dotnet(true, false))} ="
            else $"type {c.Name} ="
        let memberLines =
            match c.Returns with
            | Return.None
            | Return.Single _ -> List.empty
            | Return.Table nts ->
                nts |> List.map(fun nt -> Helpers.memberExpr nt.Name)
        let sp12 = String.init 12 (fun _ -> " ")
        let executeLines =
            [   yield $"    static member Execute(conn: ISqlConnection{NamedType.formatParameters(c.Parameters, FormatParameterOption.Dotnet(true, true))}) ="
                yield "        task {"
                yield
                    sp12 +
                    match c.CommandType with
                    | CommandTy.StoredProc ->
                        $"use command = new SqlCommand(\"{c.QualifiedName}\", conn.Connection, CommandType = CommandType.StoredProcedure)"
                    | CommandTy.UDF ->
                        match c.Returns with
                        | Return.Single _ ->
                            $"use command = new SqlCommand(\"SELECT {c.QualifiedName}({NamedType.formatParameters(c.Parameters, FormatParameterOption.Sql)})\", conn.Connection)"
                        | Return.Table _ ->
                            $"use command = new SqlCommand(\"SELECT * FROM {c.QualifiedName}({NamedType.formatParameters(c.Parameters, FormatParameterOption.Sql)})\", conn.Connection)"
                        | Return.None -> failwith "UDF with no return type"
                    | CommandTy.TableGetter ->
                        $"use command = new SqlCommand(\"SELECT * FROM {c.QualifiedName}\", conn.Connection)"
                    | CommandTy.SqlCommand sql ->
                        $"use command = new SqlCommand(\"{sql}\", conn.Connection)"
                yield sp12 + "conn.Transaction |> Option.iter(fun t -> command.Transaction <- t)"
                for p in c.Parameters do
                    yield sp12 + "command.Parameters.Add(" + NamedType.toSqlParameter(p) + ") |> ignore"
                match c.Returns with
                | Return.None ->
                    yield sp12 + "return! command.ExecuteNonQueryAsync()"
                | Return.Single(sqlp, nullable) ->
                    let readSingle =
                        let reader = "reader"
                        let read = $"{SqlType.getterStr(sqlp, reader, 0)}"
                        if nullable then
                            $"(if reader.IsDBNull(0) then ValueNone else ValueSome({read}))"
                        else
                            read
                    yield sp12 + "use! reader = command.ExecuteReaderAsync()"
                    // ToDo: handle properly case when result doesn't exist
                    yield sp12 + "reader.Read() |> ignore"
                    yield sp12 + "let result = " + readSingle
                    yield sp12 + "return result"
                | Return.Table nts ->
                    let readSingleRow = Helpers.readSingleRow(nts, c.Name)
                    yield sp12 + "use! reader = command.ExecuteReaderAsync()"
                    yield sp12 + $"let b = ImmutableArray.CreateBuilder<{c.Name}>()"
                    yield sp12 + "while reader.Read() do"
                    yield sp12 + $"    b.Add({readSingleRow})"
                    yield sp12 + "return b.ToImmutable()"
                yield "        }"

            ]
        [ yield firstLine; yield! memberLines; yield! executeLines ]

let getGeneratedFileLines(designTimeConn: string, designTimeServer: string) =

    let dbInfo = DbInfo.Get(designTimeConn, designTimeServer)

    let b = ImmutableArray.CreateBuilder<string>()

    let title(str: string) =
        let line = @"// " + String.init str.Length (fun _ -> "-")
        [
            line
            "// " + str
            line
        ] |> b.AddRange

    let opens() =
        b.Add "open System"
        b.Add "open System.Data"
        b.Add "open Microsoft.Data.SqlClient"
        b.Add "open System.Collections.Immutable"
        b.Add "open GeneratedADONET"

    b.Add @"// This code is auto-generated"

    for schema, schemaInfo in dbInfo do

        do  b.Add $"namespace GeneratedADONET.{schema}"
            opens()

        if schemaInfo.UserDefinedTableTypes |> List.isEmpty |> not then
            title "User-Defined Table Types"
            for udt in schemaInfo.UserDefinedTableTypes do
                b.AddRange(UserDefinedTableT.fSharpTypeDef udt)

        if schemaInfo.StoredProcedures |> List.isEmpty |> not then
            title "Stored Procedures"
            for tsp in schemaInfo.StoredProcedures do
                let c = Command.FromTypedStoredProcedure(tsp)
                b.AddRange(Command.format c)

        if schemaInfo.UserDefinedFunctions |> List.isEmpty |> not then
            title "User Defined Functions"
            for tudf in schemaInfo.UserDefinedFunctions do
                let c = Command.FromTypedUDF(tudf)
                b.AddRange(Command.format c)

        if schemaInfo.TableGetters |> List.isEmpty |> not then
            title "Table Getters"
            b.Add "namespace GeneratedADONET.TableGetters"
            opens()
            for tg in schemaInfo.TableGetters do
                let c = Command.GetterFromTypedTable(tg)
                b.AddRange(Command.format c)

    b.ToImmutable()