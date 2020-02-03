<?php

namespace BenTools\SqlSorter;

use BenTools\SimpleDBAL\Contract\AdapterInterface;
use Ulid\Ulid;
use function BenTools\Where\insert;
use function BenTools\Where\select;
use function Safe\json_decode;
use function Safe\json_encode;

final class MySqlJsonSorter
{
    const UTF8MB4_UNICODE_CI = 'utf8mb4_unicode_ci';
    const DEFAULT_CHARSET = self::UTF8MB4_UNICODE_CI;

    /**
     * @var AdapterInterface
     */
    private $connection;

    /**
     * MySqlJsonSorter constructor.
     */
    public function __construct(AdapterInterface $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param array $data
     * @param array $sorts
     * @param array $options
     * @return array
     * @throws \InvalidArgumentException
     */
    public function __invoke(array $data, array $sorts = [], array $options = []): array
    {

        if ([] === $sorts) {
            return $data;
        }

        $options = $options + $this->getDefaultOptions();
        $table = $this->createTable($options['charset']);
        $jsons = array_values(array_map(function ($item) {
            return ['data' => json_encode($item)];
        }, $data));
        $insert = insert(...$jsons)->into('`'.$table.'`', 'data');

        foreach ($insert->split($options['insert_buffer']) as $insertQuery) {
            $this->connection->execute((string) $insertQuery, $insertQuery->getValues());
        }

        $select = select('JSON_EXTRACT(data, "$")')->from('`'.$table.'`');

        foreach ($sorts as $path => $direction) {
            $column = sprintf('JSON_UNQUOTE(JSON_EXTRACT(data, "$.%s"))', $path);
            $castedColumn = sprintf('CAST(%s as %s)', $column, $this->mapCast($options['cast'][$path] ?? null));
            $select = $select->andOrderBy(sprintf('%s %s', $castedColumn, $direction));
        }

        $result = $this->connection->execute((string) $select, $select->getValues())->asList();

        return array_map(function ($item) {
            return json_decode($item, true);
        }, $result);
    }

    /**
     * @param string|null $type
     * @return string
     */
    private function mapCast(?string $type)
    {
        switch ($type) {
            case 'bool':
            case 'int':
                return 'SIGNED';
            case 'float':
                return 'DECIMAL(60,30)';
            default:
                return 'CHAR';
        }
    }

    /**
     * @param string $charset
     * @return string
     */
    private function createTable(string $charset): string
    {
        $tableName = (string) Ulid::generate(true);
        $this->connection->execute(sprintf('CREATE TEMPORARY TABLE `%s` (`data` JSON NOT NULL) COLLATE=%s', $tableName, $charset));
        return $tableName;
    }

    /**
     * @return array
     */
    private function getDefaultOptions()
    {
        return [
            'charset'       => self::DEFAULT_CHARSET,
            'insert_buffer' => 500,
        ];
    }
}
