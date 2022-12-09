package org.apache.flink.bilibili.udf.scalar;

import org.apache.flink.bilibili.udf.scalar.location.BaseStation;
import org.apache.flink.bilibili.udf.scalar.location.IpPart;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * 通过IP获取相应的地域位置信息
 *
 * 使用方法:
 * LOCATION({$ip}, 'province')  返回 省份信息
 * LOCATION({$ip}, 'city')  返回 城市信息
 * LOCATION({$ip}, 'classification')  返回 城市等级信息
 *
 * @author Xinux
 * @version $Id: Location.java, v 0.1 2020-02-20 3:21 PM Xinux Exp $$
 */
public class Location extends ScalarFunction {

    private static final String[] FIRST_TIER     = new String[] { "上海", "北京", "深圳", "广州" };
    private static final String[] FIRST_TIER_NEW = new String[] { "成都", "杭州", "重庆", "武汉", "苏州",
            "西安", "天津", "南京", "郑州", "长沙", "沈阳", "青岛", "宁波", "东莞", "无锡" };
    private static final String[] SECOND_TIER    = new String[] { "昆明", "大连", "厦门", "合肥", "佛山",
            "福州", "哈尔滨", "济南", "温州", "长春", "石家庄", "常州", "泉州", "南宁", "贵阳", "南昌", "南通", "金华", "徐州",
            "太原", "嘉兴", "烟台", "惠州", "保定", "台州", "中山", "绍兴", "乌鲁木齐", "潍坊", "兰州" };
    private static final String[] THIRD_TIER     = new String[] { "珠海", "镇江", "海口", "扬州", "临沂",
            "洛阳", "唐山", "呼和浩特", "盐城", "汕头", "廊坊", "泰州", "济宁", "湖州", "江门", "银川", "淄博", "邯郸", "芜湖",
            "漳州", "绵阳", "桂林", "三亚", "遵义", "咸阳", "上饶", "莆田", "宜昌", "赣州", "淮安", "揭阳", "沧州", "商丘",
            "连云港", "柳州", "岳阳", "信阳", "株洲", "衡阳", "襄阳", "南阳", "威海", "湛江", "包头", "鞍山", "九江", "大庆",
            "许昌", "新乡", "宁德", "西宁", "宿迁", "菏泽", "蚌埠", "邢台", "铜陵", "阜阳", "荆州", "驻马店", "湘潭", "滁州",
            "肇庆", "德阳", "曲靖", "秦皇岛", "潮州", "吉林", "常德", "宜春", "黄冈" };
    private static final String[] FOURTH_TIER    = new String[] { "舟山", "泰安", "孝感", "鄂尔多斯", "开封",
            "南平", "齐齐哈尔", "德州", "宝鸡", "马鞍山", "郴州", "安阳", "龙岩", "聊城", "渭南", "宿州", "衢州", "梅州", "宣城",
            "周口", "丽水", "安庆", "三明", "枣庄", "南充", "淮南", "平顶山", "东营", "呼伦贝尔", "乐山", "张家口", "清远", "焦作",
            "河源", "运城", "锦州", "赤峰", "六安", "盘锦", "宜宾", "榆林", "日照", "晋中", "怀化", "承德", "遂宁", "毕节",
            "佳木斯", "滨州", "益阳", "汕尾", "邵阳", "玉林", "衡水", "韶关", "吉安", "北海", "茂名", "延边朝鲜族自治州", "黄山",
            "阳江", "抚州", "娄底", "营口", "牡丹江", "大理白族自治州", "咸宁", "黔东南苗族侗族自治州", "安顺", "黔南布依族苗族自治州", "泸州",
            "玉溪", "通辽", "丹东", "临汾", "眉山", "十堰", "黄石", "濮阳", "亳州", "抚顺", "永州", "丽江", "漯河", "铜仁",
            "大同", "松原", "通化", "红河哈尼族彝族自治州", "内江" };
    private static final String[] FIFTH_TIER     = new String[] { "长治", "荆门", "梧州", "拉萨", "汉中",
            "四平", "鹰潭", "广元", "云浮", "葫芦岛", "本溪", "景德镇", "六盘水", "达州", "铁岭", "钦州", "广安", "保山", "自贡",
            "辽阳", "百色", "乌兰察布", "普洱", "黔西南布依族苗族自治州", "贵港", "萍乡", "酒泉", "忻州", "天水", "防城港", "鄂州",
            "锡林郭勒盟", "白山", "黑河", "克拉玛依", "临沧", "三门峡", "伊春", "鹤壁", "随州", "新余", "晋城", "文山壮族苗族自治州",
            "巴彦淖尔", "河池", "凉山彝族自治州", "乌海", "楚雄彝族自治州", "恩施土家族苗族自治州", "吕梁", "池州", "西双版纳傣族自治州", "延安",
            "雅安", "巴中", "双鸭山", "攀枝花", "阜新", "兴安盟", "张家界", "昭通", "海东", "安康", "白城", "朝阳", "绥化", "淮北",
            "辽源", "定西", "吴忠", "鸡西", "张掖", "鹤岗", "崇左", "湘西土家族苗族自治州", "林芝", "来宾", "贺州", "德宏傣族景颇族自治州",
            "资阳", "阳泉", "商洛", "陇南", "平凉", "庆阳", "甘孜藏族自治州", "大兴安岭地区", "迪庆藏族自治州", "阿坝藏族羌族自治州",
            "伊犁哈萨克自治州", "中卫", "朔州", "儋州", "铜川", "白银", "石嘴山", "莱芜", "武威", "固原", "昌吉回族自治州",
            "巴音郭楞蒙古自治州", "嘉峪关", "阿拉善盟", "阿勒泰地区", "七台河", "海西蒙古族藏族自治州", "塔城地区", "日喀则", "昌都",
            "海南藏族自治州", "金昌", "哈密", "怒江傈僳族自治州", "吐鲁番", "那曲地区", "阿里地区", "喀什地区", "阿克苏地区", "甘南藏族自治州",
            "海北藏族自治州", "山南", "临夏回族自治州", "博尔塔拉蒙古自治州", "玉树藏族自治州", "黄南藏族自治州", "和田地区", "三沙",
            "克孜勒苏柯尔克孜自治州", "果洛藏族自治州"            };
    private static final String[] SPECIAL        = new String[] { "香港", "澳门" };

    private static final Logger LOG            = LoggerFactory.getLogger(Location.class);
    private static BaseStation db             = null;
    private static BaseStation    dbv6           = null;

    public void open(FunctionContext context) {

        LOG.info("Instantiating Location UDF.");

        if (db == null) {
            LOG.info("ipv4 loading.");
            try {
                db = new BaseStation("/application/installs/ipip.ipdb");
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
            LOG.info("ipv4 loaded.");
        }

        if (dbv6 == null) {
            LOG.info("ipv6 loading.");
            try {
                dbv6 = new BaseStation("/application/installs/ipip_v6.ipdb");
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
            LOG.info("ipv6 loaded.");
        }
    }

    /**
     * 输入ip和类别,输出地域信息
     * @param ip
     * @param part
     * @return
     */
    public String eval(String ip, String part) {

        if (db == null || dbv6 == null) {
            LOG.error("no db");
            return null;
        }

        // 输入参数为空
        if (ip == null || part == null) {
            return null;

            // 不为空
        } else {

            String result;

            // 需要截取的结果字段
            IpPart ipPart = IpPart.parse(part.toUpperCase());

            String[] ipInfo;

            try {

                // 判断v6 or v4
                if (ip.indexOf(":") > 0) {
                    ipInfo = dbv6.find(ip, "CN");
                } else {
                    ipInfo = db.find(ip, "CN");
                }

                // 结果为空
                if (ipInfo == null)
                    return null;

                // 获取截取idx
                int partIdx = ipPart.getIdx();
                boolean isClassification = false; // 默认不是城市等级

                // 当是城市等级时
                if (partIdx == -1) {

                    // 排除海外，返回
                    if (!ipInfo[IpPart.COUNTRY.getIdx()].equals("中国")) {
                        result = "海外";
                        return result;
                    }

                    // 排除台湾，返回
                    if (ipInfo[IpPart.PROVINCE.getIdx()].equals("台湾")) {
                        result = "港澳台";
                        return result;
                    }

                    // 设置标记为true
                    isClassification = true;
                    // 调整获取字段为city
                    partIdx = 2;

                }

                // 获取字段值
                String r = ipInfo[partIdx];

                // 当字段为city，且city值为空
                if (partIdx == 2 && (r == null || r.isEmpty())) {

                    // 获取其省份
                    String province = ipInfo[IpPart.PROVINCE.getIdx()];

                    // 如果是省与城市同值的地区
                    if (province.equals("北京") || province.equals("上海") || province.equals("天津")
                        || province.equals("重庆") || province.equals("香港") || province.equals("澳门")) {

                        r = province;

                        // 如果不是，并且查询内容是城市等级，返回
                    } else if (isClassification) {
                        result = "其他";
                        return result;
                    }

                }

                // 设置结果值
                // 如果查询内容是城市等级，设置等级值
                if (isClassification) {
                    if (Arrays.asList(FIRST_TIER).contains(r)) {
                        result = "一线";
                    } else if (Arrays.asList(FIRST_TIER_NEW).contains(r)) {
                        result = "新一线";
                    } else if (Arrays.asList(SECOND_TIER).contains(r)) {
                        result = "二线";
                    } else if (Arrays.asList(THIRD_TIER).contains(r)) {
                        result = "三线";
                    } else if (Arrays.asList(FOURTH_TIER).contains(r)) {
                        result = "四线";
                    } else if (Arrays.asList(FIFTH_TIER).contains(r)) {
                        result = "五线";
                    } else if (Arrays.asList(SPECIAL).contains(r)) {
                        result = "港澳台";
                    } else {
                        result = "其他";
                    }

                    // 如果不是，直接设置字段值
                } else {
                    result = r;
                }

                return result;
            } catch (Exception e) {
                LOG.error(e.getMessage());
                return null;
            }
        }

    }

}
