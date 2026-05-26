const API = 'https://port-0-admichat-lzgmwhc4d9883c97.sel4.cloudtype.app';

const REFS = [
    { title: "Yogibo X ROBLOX", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/upload/NNEditor/20250813/copy-1755066836-people.png","https://yogibo.kr/web/upload/NNEditor/20250813/copy-1755066847-color.png","https://yogibo.kr/web/upload/NNEditor/20250813/copy-1755066857-people2.png"] },
    { title: "Yogibo X 여의도 봄꽃축제", cat: "문화·전시", imgs: ["https://yogibo.kr/web/img/event/0408/img_001.jpg","https://yogibo.kr/web/img/event/0408/img_002.jpg","https://yogibo.kr/web/img/event/0408/img_003.jpg","https://yogibo.kr/web/img/event/0408/img_005.jpg","https://yogibo.kr/web/img/event/0408/img_006.jpg"] },
    { title: "Yogibo X 삼성라이온즈 (2025)", cat: "스포츠", imgs: ["https://yogibo.kr/web/img/event/0408/img_01.png","https://yogibo.kr/web/img/event/0408/img_02.jpg","https://yogibo.kr/web/img/event/0408/img_03.png","https://yogibo.kr/web/img/event/0408/img_04.jpg","https://yogibo.kr/web/img/event/0408/img_05.jpg"] },
    { title: "Yogibo X 탼", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/img/event/0409/img_01.png","https://yogibo.kr/web/img/event/0409/img_02.jpg","https://yogibo.kr/web/img/event/0409/img_03.jpg","https://yogibo.kr/web/img/event/0409/img_04.jpg"] },
    { title: "Yogibo X 롯데호텔 월드", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/img/news/2024/05/30/img_01.png","https://yogibo.kr/web/img/news/2024/05/30/img_02.jpg","https://yogibo.kr/web/img/news/2024/05/30/img_03.jpg","https://yogibo.kr/web/img/news/2024/05/30/img_04.jpg"] },
    { title: "Yogibo X 레노부르크 뮤지엄", cat: "문화·전시", imgs: ["https://yogibo.kr/web/img/board/news/1115/img_01.jpg","https://yogibo.kr/web/img/board/news/1115/img_02.jpg","https://yogibo.kr/web/img/board/news/1115/img_03.jpg","https://yogibo.kr/web/img/board/news/1115/img_04.jpg","https://yogibo.kr/web/img/board/news/1115/img_05.jpg"] },
    { title: "Yogibo X 빛의시어터", cat: "문화·전시", imgs: ["https://yogibo.kr/web/img/news/2024/04/img_01.jpg","https://yogibo.kr/web/img/news/2024/04/img_02.jpg","https://yogibo.kr/web/img/news/2024/04/img_03.jpg","https://yogibo.kr/web/img/news/2024/04/img_04.jpg","https://yogibo.kr/web/img/news/2024/04/img_05.jpg"] },
    { title: "Yogibo X 인천 대한항공 점보스", cat: "스포츠", imgs: ["https://yogibo.kr/web/img/news/2024/03/04/img_01.png","https://yogibo.kr/web/img/news/2024/03/04/img_02.png","https://yogibo.kr/web/img/news/2024/03/04/img_03.png","https://yogibo.kr/web/img/news/2024/03/04/img_04.png"] },
    { title: "Yogibo X SSG랜더스", cat: "스포츠", imgs: ["https://yogibo.kr/web/img/board/news/1019/img_001.jpg","https://yogibo.kr/web/img/board/news/1019/img_002.jpg","https://yogibo.kr/web/img/board/news/1019/img_003.jpg","https://yogibo.kr/web/img/board/news/1019/img_004.jpg"] },
    { title: "Yogibo X 성남 FC", cat: "스포츠", imgs: ["https://yogibo.kr/web/img/news/2023/10/11/img_01.jpg","https://yogibo.kr/web/img/news/2023/10/11/img_02.jpg","https://yogibo.kr/web/img/news/2023/10/11/img_03.jpg","https://yogibo.kr/web/img/news/2023/10/11/img_05.jpg"] },
    { title: "Yogibo X PAGODA", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/img/news/2023/09/27/banner_01.jpg","https://yogibo.kr/web/img/news/2023/09/27/banner_02.jpg","https://yogibo.kr/web/img/news/2023/09/27/banner_03.jpg","https://yogibo.kr/web/img/news/2023/09/27/banner_04.jpg"] },
    { title: "Yogibo X 금호강 바람소리길 축제", cat: "문화·전시", imgs: ["https://yogibo.kr/web/img/board/news/0922/board_01.jpg","https://yogibo.kr/web/img/board/news/0922/board_02.jpg","https://yogibo.kr/web/img/board/news/0922/board_03.jpg"] },
    { title: "Yogibo X 다방", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/img/board/news/0824/da_01.jpg","https://yogibo.kr/web/img/board/news/0824/da_02.jpg","https://yogibo.kr/web/img/board/news/0824/da_03.jpg","https://yogibo.kr/web/img/board/news/0824/da_04.jpg"] },
    { title: "Yogibo X VOCO 서울 강남", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/img/news/2023/08/img_01.jpg","https://yogibo.kr/web/img/news/2023/08/img_002.png","https://yogibo.kr/web/img/news/2023/08/img_04.jpg"] },
    { title: "Yogibo X CIMER", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/img/board/0802/se_01.jpg","https://yogibo.kr/web/img/board/0802/se_02.jpg","https://yogibo.kr/web/img/board/0802/se_03.jpg"] },
    { title: "Yogibo X SONO PET", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/img/board/0802/sono_01.png","https://yogibo.kr/web/img/board/0802/sono_02.png","https://yogibo.kr/web/img/board/0802/sono_03.png"] },
    { title: "Yogibo X 검암역 로얄파크시티 푸르지오", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/img/board/0802/gum_01.jpg","https://yogibo.kr/web/img/board/0802/gum_02.png","https://yogibo.kr/web/img/board/0802/gum_03.png"] },
    { title: "Yogibo x 휘닉스평창", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/img/board/0802/h_01.jpg","https://yogibo.kr/web/img/board/0802/h_02.jpg","https://yogibo.kr/web/img/board/0802/h_03.jpg"] },
    { title: "Yogibo X 더현대 대구", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/img/board/0517/banner_01.jpg","https://yogibo.kr/web/img/board/0517/banner_02.jpg","https://yogibo.kr/web/img/board/0517/banner_03.jpg"] },
    { title: "Yogibo X 현대캐피탈 스카이워커스", cat: "스포츠", imgs: ["https://yogibo.kr/web/img/board/0116/1.jpg","https://yogibo.kr/web/img/board/0116/2.jpg","https://yogibo.kr/web/img/board/0116/3.jpg","https://yogibo.kr/web/img/board/0116/4.jpg"] },
    { title: "Yogibo X LG 세이커스", cat: "스포츠", imgs: ["https://yogibo.kr/web/img/board/0110/1.jpg","https://yogibo.kr/web/img/board/0110/3.jpg","https://yogibo.kr/web/img/board/0110/2-1.jpg","https://yogibo.kr/web/img/board/0110/2-2.jpg"] },
    { title: "Yogibo X CONRAD SEOUL", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20221025/copy-1666659904-Playful_Conrad_PKG_My_Fluffy_Friend_Horizontal.jpg","https://yogibo.kr/web/upload/NNEditor/20221025/image.png"] },
    { title: "Yogibo X 포뮬러 E", cat: "스포츠", imgs: ["https://yogibo.kr/web/upload/NNEditor/20220817/AKR20220504072500007_01_i_P4.jpg","https://yogibo.kr/web/upload/NNEditor/20220817/12-MALC0211.jpg","https://yogibo.kr/web/upload/NNEditor/20220817/SE-012217ba-6200-4f0e-b057-1f90e2611f11.jpg"] },
    { title: "Yogibo X 부천국제판타스틱영화제", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20220803/01.jpg","https://yogibo.kr/web/upload/NNEditor/20220803/02.jpg","https://yogibo.kr/web/upload/NNEditor/20220803/03.jpg"] },
    { title: "Yogibo X 원주DB프로미", cat: "스포츠", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211018/SE-8e32853e-5f92-4295-a8cc-d3f0996bef5c.jpg","https://yogibo.kr/web/upload/NNEditor/20211018/SE-da4c7f74-4747-48e6-bd85-d23922b5b5ff.jpg","https://yogibo.kr/web/upload/NNEditor/20211018/SE-06e1b03c-98d5-4af6-8737-da0665d7e8a9.jpg"] },
    { title: "Yogibo X CGV", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211028/01.jpg","https://yogibo.kr/web/upload/NNEditor/20211028/04.jpg","https://yogibo.kr/web/upload/NNEditor/20211028/05.jpg"] },
    { title: "Yogibo X 파라다이스호텔 CIMER", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211206/01.png","https://yogibo.kr/web/upload/NNEditor/20220608/SE-8370ddf7-dfeb-491e-8a33-7db1c5b52a50.png","https://yogibo.kr/web/upload/NNEditor/20211206/06.jpg"] },
    { title: "Yogibo X JW 메리어트 호텔 서울", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211203/seljw-exterior-5640-hor-wide.jpg","https://yogibo.kr/web/upload/NNEditor/20211203/20211105EFBCBF153223.jpg"] },
    { title: "Yogibo X 롯데월드타워", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20210426/IMG_0654.JPG","https://yogibo.kr/web/upload/NNEditor/20210426/IMG_0687.JPG","https://yogibo.kr/web/upload/NNEditor/20210426/IMG_0745.JPG"] },
    { title: "Yogibo X 라시따 델라모다", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211217/1.jpeg","https://yogibo.kr/web/upload/NNEditor/20211217/4.jpeg"] },
    { title: "Yogibo X 라한셀렉트", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211208/01.jpg","https://yogibo.kr/web/upload/NNEditor/20211208/02.jpg","https://yogibo.kr/web/upload/NNEditor/20211208/03.jpg"] },
    { title: "Yogibo X 삼성라이온즈 (2021)", cat: "스포츠", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211206/copy-1638784874-01.jpg","https://yogibo.kr/web/upload/NNEditor/20211206/copy-1638784878-02.jpg","https://yogibo.kr/web/upload/NNEditor/20211206/copy-1638784883-03.jpg"] },
    { title: "Yogibo X 휘닉스 평창", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/1-2.jpg","https://yogibo.kr/web/upload/NNEditor/20211220/1-4.jpg","https://yogibo.kr/web/upload/NNEditor/20211220/1-6.jpg"] },
    { title: "Yogibo X 보롬왓", cat: "여행·숙박", imgs: ["https://gi.esmplus.com/yogibo/image/cafe24/b2b/%EC%A7%A0%EB%82%B4%ED%88%AC%EC%96%B4/icannotknow_2_9_2019_13_38_41_21.jpg","https://gi.esmplus.com/yogibo/image/cafe24/b2b/%EC%A7%A0%EB%82%B4%ED%88%AC%EC%96%B4/1-7.jpg"] },
    { title: "Yogibo X 아브뉴프랑", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211206/ED8C90EAB59001.jpg","https://yogibo.kr/web/upload/NNEditor/20211206/ED8C90EAB59002.jpg","https://yogibo.kr/web/upload/NNEditor/20211206/EAB491EBAA8501.jpg"] },
    { title: "Yogibo X 스틸북스", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211221/stillbooks01.jpg","https://yogibo.kr/web/upload/NNEditor/20211221/stillbooks04.jpg"] },
    { title: "Yogibo X 쓰리잘비", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/02_shop1_180737.jpg","https://yogibo.kr/web/upload/NNEditor/20211220/03_shop1_180737.jpg"] },
    { title: "Yogibo X AK PLAZA", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/upload/NNEditor/20200511/20200507_105249_shop1_102849.jpg","https://yogibo.kr/web/upload/NNEditor/20200506/20200506_153923_shop1_154844.jpg"] },
    { title: "Yogibo X 제주투브이알", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211216/01.png","https://yogibo.kr/web/upload/NNEditor/20211216/02.png"] },
    { title: "Yogibo X 파라다이스시티 원더박스", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211223/99034A3A5DF2F19139_shop1_173959.png","https://yogibo.kr/web/upload/NNEditor/20211223/999B44415DF2F19134_shop1_173958.png"] },
    { title: "Yogibo X 헤이홈", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/01_shop1_151808.png","https://yogibo.kr/web/upload/NNEditor/20211220/02_shop1_151808.png"] },
    { title: "Yogibo X 애플트리 키즈카페", cat: "쇼핑·팝업", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/01_shop1_174450.png","https://yogibo.kr/web/upload/NNEditor/20211220/02_shop1_174450.jpg"] },
    { title: "Yogibo X 라이즈호텔", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211223/DSCF2165_shop1_160434.jpg","https://yogibo.kr/web/upload/NNEditor/20211223/DSCF2172_shop1_160434.jpg"] },
    { title: "Yogibo X 현대캐피탈스카이워커스", cat: "스포츠", imgs: ["https://yogibo.kr/web/upload/NNEditor/20200110/KOK00360_size_shop1_091746.jpg","https://yogibo.kr/web/upload/NNEditor/20200110/%EB%B8%94%EB%9F%AC%ED%8E%B8%EC%A7%91_shop1_091746.jpg"] },
    { title: "Yogibo x 국립현대미술관", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211223/DSC_2365_shop1_152311.jpg","https://yogibo.kr/web/upload/NNEditor/20211223/DSC_1319_shop1_152311.jpg","https://yogibo.kr/web/upload/NNEditor/20211223/_O7A9636_shop1_152311.jpg"] },
    { title: "Yogibo X 파라다이스 시티", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211224/EC9A94EAB8B0EBB3B4_ED8C8CEB9DBCEB8BA4EC9DB4EC8AA4EC8B9CED8BB0_ED948CEB9DBCEC9E902028229_shop1_172411.png","https://yogibo.kr/web/upload/NNEditor/20211224/EC9A94EAB8B0EBB3B4_ED8C8CEB9DBCEB8BA4EC9DB4EC8AA4EC8B9CED8BB0_ED948CEB9DBCEC9E902028129_shop1_172410.png"] },
    { title: "Yogibo X 코믹콘", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211223/ECBD94EBAFB9ECBD982_shop1_180313.png","https://yogibo.kr/web/upload/NNEditor/20211223/ECBD94EBAFB9ECBD941_shop1_180313.png"] },
    { title: "Yogibo X 아난티 코브", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/EC9584EB829CED8BB0_shop1_175058.png","https://yogibo.kr/web/upload/NNEditor/20211220/EC9584EB829CED8BB01_shop1_175059.png"] },
    { title: "Yogibo X 인터컨티넨탈 호텔", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/C0001.00_02_00_18.EC8AA4ED8BB8011_shop1_170723.png","https://yogibo.kr/web/upload/NNEditor/20211220/DSC01258_shop1_170725.jpg"] },
    { title: "Yogibo X 디뮤지엄", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/DSC00711_shop1_160439.png","https://yogibo.kr/web/upload/NNEditor/20211220/DSC00733_shop1_160502.png"] },
    { title: "Yogibo X CGV그린시네마", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/91c3a7cfab7a6d389f6b506ecb1018c034fc3dbb05fcc922552ac903c26ae14f_shop1_170024.jpg","https://yogibo.kr/web/upload/NNEditor/20211220/CGV_shop1_170030.png"] },
    { title: "Yogibo X 부천국제영화제", cat: "문화·전시", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/DSC00920_1_shop1_164658.png","https://yogibo.kr/web/upload/NNEditor/20211220/DSC00979HH_shop1_164659.png"] },
    { title: "Yogibo x 테르메덴", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211220/0de8a153762e26996dd238fb00dfca64796c7d00d7e5702ca7c1ad25556bce51_shop1_162733.jpg","https://yogibo.kr/web/upload/NNEditor/20211220/87a3b72fa2110073501fc432742f3d8d1fa39f10f6688534b2980711dd81d4c6_shop1_162733.jpg"] },
    { title: "Yogibo X 설해원", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211216/EC84A4ED95B4EC9B9028329_shop1_170618.png","https://yogibo.kr/web/upload/NNEditor/20211216/EC84A4ED95B4EC9B9028129_shop1_170618.png"] },
    { title: "Yogibo X 웅진씽크빅", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211224/woongin0.jpg","https://yogibo.kr/web/upload/NNEditor/20211224/woongin1.jpg","https://yogibo.kr/web/upload/NNEditor/20211224/woongin2.jpg"] },
    { title: "Yogibo X 현대카드", cat: "기업·오피스", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211216/DSC01275_shop1_160057.jpg","https://yogibo.kr/web/upload/NNEditor/20211216/DSC01856_shop1_160057.jpg"] },
    { title: "Yogibo x 쏠비치 양양", cat: "여행·숙박", imgs: ["https://yogibo.kr/web/upload/NNEditor/20211223/EB8C80EBAA85EC8FA0EBB984ECB998_ED9988ED8E98EC9DB4ECA780EC82ACECA784_1_shop1_154114.jpg","https://yogibo.kr/web/upload/NNEditor/20211223/EB8C80EBAA85EC8FA0EBB984ECB998_ED9988ED8E98EC9DB4ECA780EC82ACECA784_2_shop1_154118.jpg"] },
];

async function deleteAll() {
    console.log('Fetching existing boards...');
    const res = await fetch(`${API}/api/b2b/boards`);
    const json = await res.json();
    if (!json.success) return console.log('Failed to fetch');
    console.log(`Deleting ${json.data.length} records...`);
    for (const board of json.data) {
        await fetch(`${API}/api/b2b/board/${board._id}`, { method: 'DELETE' });
        process.stdout.write('.');
    }
    console.log('\nAll deleted!');
}

async function insertAll() {
    console.log('Inserting new records with original yogibo.kr URLs...');
    let count = 0;
    for (const ref of REFS) {
        const fd = new FormData();
        fd.append('title', ref.title);
        fd.append('category', ref.cat);
        fd.append('existingImages', JSON.stringify(ref.imgs));
        const res = await fetch(`${API}/api/b2b/board`, { method: 'POST', body: fd });
        if (res.ok) { count++; console.log(`  OK: ${ref.title}`); }
        else console.log(`  FAIL: ${ref.title} - ${res.status}`);
    }
    console.log(`Done! Inserted ${count} records.`);
}

deleteAll().then(insertAll).catch(console.error);
