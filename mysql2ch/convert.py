import sqlparse
from sqlparse.sql import Function, Identifier
from sqlparse.sql import Token as SQLToken
from sqlparse.sql import TokenList
from sqlparse.tokens import Keyword, Token, Whitespace


class SqlConvert:
    _type_mapping = {
        "date": "Date",
        "datetime": "DateTime",
        "bool": "UInt8",
        "float": "Float32",
        "double": "Float64",
        "varchar": "String",
        "decimal": "Decimal{digits}",
        "tinyint": "Int8",
        "int": "Int32",
        "smallint": "Int16",
        "mediumint": "Int32",
        "bigint": "Int64",
        "timestamp": "DateTime",
        "char": "FixedString",
        "bigchar": "String",
    }

    @classmethod
    def _add_token(cls, schema, parsed, tokens, token_list):
        for i, token in enumerate(tokens):
            if token.value == "add" and parsed.token_next(i)[1].value != "column":
                token_list.tokens.append(token)
                token_list.tokens.append(SQLToken(Whitespace, " "))
                token_list.tokens.append(SQLToken(Keyword, "column"))
            elif token.value == "change":
                token_list.tokens.append(SQLToken(Keyword, "rename"))
                token_list.tokens.append(SQLToken(Whitespace, " "))
                token_list.tokens.append(SQLToken(Keyword, "column"))
                token_list.tokens.append(SQLToken(Whitespace, " "))
                tokens = parsed.token_next(i)[1].tokens
                token_list.tokens.append(tokens[0])
                token_list.tokens.append(SQLToken(Whitespace, " "))
                token_list.tokens.append(SQLToken(Keyword, "to"))
                token_list.tokens.append(SQLToken(Whitespace, " "))
                token_list.tokens.append(tokens[2])
                return token_list
            elif isinstance(token, (Function, Identifier)) or token.ttype == Token.Name.Builtin:
                if token.ttype == Token.Name.Builtin:
                    token_list.tokens.append(SQLToken(Keyword, cls._type_mapping.get(token.value)))
                elif isinstance(token, Identifier):
                    if parsed.token_prev(i - 1)[1].value == "table":
                        value = token.value
                        table = (
                            f"{schema}.{token.value}" if len(value.split(".")) == 1 else token.value
                        )
                        token_list.tokens.append(SQLToken(Keyword, table))
                    elif len(token.tokens) == 1:
                        real_token = cls._type_mapping.get(token.value)
                        token_list.tokens.append(
                            SQLToken(Keyword, real_token) if real_token else token
                        )
                    else:
                        cls._add_token(schema, parsed, token.tokens, token_list)
                else:
                    len_token = len(token.tokens)
                    if len_token == 3:
                        identifier, _, digits = token.tokens
                    else:
                        identifier, digits = token.tokens
                    if identifier.value == "decimal":
                        token_list.tokens.append(
                            SQLToken(
                                Keyword,
                                cls._type_mapping.get(identifier.value).format(digits=digits),
                            )
                        )
                    else:
                        token_list.tokens.append(
                            SQLToken(Keyword, cls._type_mapping.get(identifier.value))
                        )
            elif token.value in ["null", "not null"]:
                continue
            elif token.ttype == Whitespace and i > 0 and token_list.tokens[-1].ttype == Whitespace:
                continue
            else:
                token_list.tokens.append(token)
        return token_list

    @classmethod
    def to_clickhouse(cls, schema: str, query: str):
        """
        parse ddl query
        :param schema:
        :param query:
        :return:
        """
        token_list = TokenList()
        parsed = sqlparse.parse(query)[0]
        token_list = cls._add_token(schema, parsed, parsed.tokens, token_list)
        return str(token_list)
